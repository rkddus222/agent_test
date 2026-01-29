import React, { useState } from 'react'
import axios from 'axios'
import '../components/SMQTest.css'

function SMQTest() {
  const [smqInput, setSmqInput] = useState('{"metrics": ["total_brn_cnt"], "groupBy": ["branch__brn_stcd"], "filters": [], "orderBy": [], "limit": 100, "joins": []}')
  const [copyFeedback, setCopyFeedback] = useState(null)
  const [result, setResult] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)
  const [executing, setExecuting] = useState(false)
  const [executeResult, setExecuteResult] = useState(null)
  const [executeError, setExecuteError] = useState(null)

  const handleConvert = async () => {
    if (!smqInput.trim()) {
      setError('SMQ를 입력해주세요.')

      return
    }

    setLoading(true)
    setError(null)
    setResult(null)

    try {
      // JSON 문자열을 파싱하여 검증
      let parsedSmq
      try {
        parsedSmq = JSON.parse(smqInput.trim())
      } catch (parseError) {
        setError(`유효하지 않은 JSON 형식입니다: ${parseError.message}`)
        setLoading(false)
        return
      }

      // 파싱된 JSON을 다시 문자열로 변환하여 전송 (백엔드가 문자열을 기대함)
      const response = await axios.post('/api/smq/convert', {
        smq: JSON.stringify(parsedSmq),
        dialect: 'oracle'
      })

      if (response.data.success) {
        const newResult = {
          sql: response.data.sql,
          metadata: response.data.metadata,
          all_queries: response.data.all_queries
        }
        setResult(newResult)
        setError(null)
        
        // 변환 성공 후 자동으로 실행
        if (response.data.sql) {
          // 약간의 지연 후 실행 (UI 업데이트를 위해)
          setTimeout(() => {
            handleExecuteWithSQL(response.data.sql)
          }, 100)
        }
      } else {
        // SMQ 변환 실패 시 명확한 에러 메시지 표시
        const errorMessage = response.data.error || 'SMQ 변환에 실패했습니다.'
        setError(`❌ 실패: ${errorMessage}`)
        setResult(null)
      }
    } catch (err) {
      // HTTP 에러 또는 네트워크 에러 처리
      let errorMessage = 'SMQ 변환 중 오류가 발생했습니다.'
      
      if (err.response) {
        // 서버에서 응답을 받았지만 에러 상태 코드인 경우
        if (err.response.status === 404) {
          errorMessage = '❌ 실패: API 엔드포인트를 찾을 수 없습니다. 백엔드 서버를 재시작해주세요.'
        } else if (err.response.data) {
          // 백엔드에서 반환한 에러 메시지 사용
          errorMessage = `❌ 실패: ${err.response.data.detail || err.response.data.error || err.message || errorMessage}`
        } else {
          errorMessage = `❌ 실패: ${err.message || errorMessage}`
        }
      } else if (err.request) {
        // 요청은 보냈지만 응답을 받지 못한 경우
        errorMessage = '❌ 실패: 서버에 연결할 수 없습니다. 백엔드 서버가 실행 중인지 확인해주세요.'
      } else {
        // 요청 설정 중 에러가 발생한 경우
        errorMessage = `❌ 실패: ${err.message || errorMessage}`
      }
      
      setError(errorMessage)
      setResult(null)
      console.error('SMQ 변환 오류:', err)
    } finally {
      setLoading(false)
    }
  }

  const handleClear = () => {
    setSmqInput('')
    setResult(null)
    setError(null)
  }

  const handleBlur = () => {
    // 포커스를 잃을 때 자동으로 JSON 포맷팅
    if (!smqInput.trim()) {
      return
    }

    try {
      // JSON 파싱 및 포맷팅
      const parsed = JSON.parse(smqInput.trim())
      const formatted = JSON.stringify(parsed, null, 2)
      setSmqInput(formatted)
      setError(null)
    } catch (parseError) {
      // 포맷팅 실패해도 에러를 표시하지 않음 (사용자가 입력 중일 수 있음)
      // 에러는 변환 시에만 표시
    }
  }

  const handleCopy = (text, id) => {
    if (!text) return
    navigator.clipboard.writeText(typeof text === 'string' ? text : JSON.stringify(text, null, 2))
      .then(() => {
        setCopyFeedback(id)
        setTimeout(() => setCopyFeedback(null), 2000)
      })
      .catch(err => {
        console.error('클립보드 복사 실패:', err)
        alert('복사에 실패했습니다.')
      })
  }

  const handleExecuteWithSQL = async (sql) => {
    if (!sql) {
      setExecuteError('실행할 SQL 쿼리가 없습니다.')
      return
    }

    setExecuting(true)
    setExecuteError(null)
    setExecuteResult(null)

    try {
      const response = await axios.post('/api/smq/execute', {
        sql: sql
      })

      if (response.data.success) {
        setExecuteResult({
          columns: response.data.columns,
          rows: response.data.rows,
          row_count: response.data.row_count
        })
        setExecuteError(null)
      } else {
        setExecuteError(response.data.error || 'SQL 실행에 실패했습니다.')
        setExecuteResult(null)
      }
    } catch (err) {
      let errorMessage = 'SQL 실행 중 오류가 발생했습니다.'
      
      if (err.response) {
        if (err.response.data) {
          errorMessage = err.response.data.error || err.response.data.detail || err.message || errorMessage
        } else {
          errorMessage = err.message || errorMessage
        }
      } else if (err.request) {
        errorMessage = '서버에 연결할 수 없습니다. 백엔드 서버가 실행 중인지 확인해주세요.'
      } else {
        errorMessage = err.message || errorMessage
      }
      
      setExecuteError(errorMessage)
      setExecuteResult(null)
      console.error('SQL 실행 오류:', err)
    } finally {
      setExecuting(false)
    }
  }

  const handleExecute = async () => {
    if (!result || !result.sql) {
      setExecuteError('실행할 SQL 쿼리가 없습니다.')
      return
    }
    await handleExecuteWithSQL(result.sql)
  }

  return (
    <div className="smq-test-page">

      <div className="smq-test-header">
        <h2>🔍 SMQ 테스트</h2>
        <p>SMQ를 JSON 형식으로 입력하면 SQL 쿼리로 변환됩니다. 입력 후 포커스를 잃으면 자동으로 포맷팅됩니다.</p>
      </div>

      <div className="smq-test-content">
        <div className="smq-test-input-section">
          <div className="smq-test-input-header">
            <label>SMQ 입력 (JSON 형식)</label>
            <div className="smq-test-buttons">
              <button
                className="smq-test-button smq-test-button-primary"
                onClick={handleConvert}
                disabled={loading || !smqInput.trim()}
              >
                {loading ? '변환 중...' : '변환'}
              </button>
              <button
                className="smq-test-button smq-test-button-secondary"
                onClick={handleClear}
                disabled={loading}
              >
                초기화
              </button>
            </div>
          </div>
          <textarea
            className="smq-test-textarea"
            value={smqInput}
            onChange={(e) => setSmqInput(e.target.value)}
            onBlur={handleBlur}
            placeholder='예시: {"metrics": ["total_brn_cnt"], "groupBy": ["branch__brn_stcd"], "filters": [], "orderBy": [], "limit": 100, "joins": []}'
            disabled={loading}
          />
        </div>
        <div className="smq-test-output-section">
          <div className="smq-test-output-header">
            <label>변환 결과</label>
          </div>
          <div className="smq-test-output-content">
            {loading && (
              <div className="smq-test-loading">변환 중...</div>
            )}
            {error && (
              <div className="smq-test-error">
                <strong>❌ 변환 실패</strong>
                <pre>{error}</pre>
              </div>
            )}
            {result && !loading && (
              <div className="smq-test-result">
                {/* 실행 결과를 상단에 표시 */}
                {executeError && (
                  <div className="smq-test-result-section">
                    <h3>실행 오류</h3>
                    <div className="smq-test-error">
                      <pre>{executeError}</pre>
                    </div>
                  </div>
                )}
                
                {executeResult && (
                  <div className="smq-test-result-section">
                    <h3>실행 결과 {executeResult.row_count !== null && executeResult.row_count !== undefined && `(${executeResult.row_count}행)`}</h3>
                    {executeResult.columns && executeResult.rows ? (
                      <div className="smq-test-table-wrapper">
                        <table className="smq-test-table">
                          <thead>
                            <tr>
                              {executeResult.columns.map((col, idx) => (
                                <th key={idx}>{col}</th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {executeResult.rows.map((row, rowIdx) => (
                              <tr key={rowIdx}>
                                {row.map((cell, cellIdx) => (
                                  <td key={cellIdx}>{cell !== null && cell !== undefined ? String(cell) : 'NULL'}</td>
                                ))}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    ) : (
                      <div className="smq-test-success-message">
                        쿼리가 성공적으로 실행되었습니다. {executeResult.row_count !== null && executeResult.row_count !== undefined && `영향받은 행: ${executeResult.row_count}`}
                      </div>
                    )}
                  </div>
                )}
                
                {/* SQL 쿼리 섹션 */}
                <div className="smq-test-result-section">
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
                    <h3 style={{ margin: 0 }}>SQL 쿼리</h3>
                    <button
                      className="smq-test-button smq-test-button-primary"
                      onClick={handleExecute}
                      disabled={executing || !result.sql}
                      style={{ padding: '0.375rem 0.75rem', fontSize: '0.875rem' }}
                    >
                      {executing ? '실행 중...' : '실행'}
                    </button>
                  </div>
                  <div className="code-block-wrapper">
                    <button 
                      className={`copy-button-absolute ${copyFeedback === 'sql' ? 'copied' : ''}`}
                      onClick={() => handleCopy(result.sql, 'sql')}
                      title="SQL 복사"
                    >
                      {copyFeedback === 'sql' ? '✅ 복사되었습니다' : '📋'}
                    </button>
                    <pre className="smq-test-sql">{result.sql}</pre>
                  </div>
                </div>
                
                {result.metadata && result.metadata.length > 0 && (
                  <div className="smq-test-result-section">
                    <h3>메타데이터</h3>
                    <div className="code-block-wrapper">
                      <button 
                        className={`copy-button-absolute ${copyFeedback === 'metadata' ? 'copied' : ''}`}
                        onClick={() => handleCopy(result.metadata, 'metadata')}
                        title="메타데이터 복사"
                      >
                        {copyFeedback === 'metadata' ? '✅ 복사되었습니다' : '📋'}
                      </button>
                      <pre className="smq-test-metadata">
                        {JSON.stringify(result.metadata, null, 2)}
                      </pre>
                    </div>
                  </div>
                )}
              </div>
            )}

            {!result && !error && !loading && (
              <div className="smq-test-placeholder">
                변환된 결과가 여기에 표시됩니다.
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

export default SMQTest

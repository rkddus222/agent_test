import React, { useState } from 'react'
import axios from 'axios'
import './SMQTest.css'

function SMQTest() {
  const [smqInput, setSmqInput] = useState('{"metrics": ["total_brn_cnt"], "groupBy": ["branch_brn_stcd"], "filters": [], "orderBy": [], "limit": 100, "joins": []}')
  const [result, setResult] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const handleConvert = async () => {
    if (!smqInput.trim()) {
      setError('SMQ를 입력해주세요.')
      return
    }

    setLoading(true)
    setError(null)
    setResult(null)

    try {
      const response = await axios.post('/api/smq/convert', {
        smq: smqInput,
        dialect: 'oracle'
      })

      if (response.data.success) {
        setResult({
          sql: response.data.sql,
          metadata: response.data.metadata,
          all_queries: response.data.all_queries
        })
      } else {
        setError(response.data.error || 'SMQ 변환에 실패했습니다.')
      }
    } catch (err) {
      if (err.response?.status === 404) {
        setError('API 엔드포인트를 찾을 수 없습니다. 백엔드 서버를 재시작해주세요.')
      } else {
        setError(err.response?.data?.detail || err.response?.data?.error || err.message || 'SMQ 변환 중 오류가 발생했습니다.')
      }
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

  return (
    <div className="smq-test-container">
      <div className="smq-test-header">
        <h2>SMQ to SQL 변환 테스트</h2>
        <p>SMQ(JSON 형식)를 입력하면 SQL 쿼리로 변환됩니다.</p>
      </div>
      <div className="smq-test-content">
        <div className="smq-test-input-section">
          <div className="smq-test-input-header">
            <label>SMQ 입력</label>
            <div className="smq-test-buttons">
              <button
                className="smq-test-button smq-test-button-primary"
                onClick={handleConvert}
                disabled={loading}
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
                <strong>오류:</strong>
                <pre>{error}</pre>
              </div>
            )}
            {result && !loading && (
              <div className="smq-test-result">
                <div className="smq-test-result-section">
                  <h3>SQL 쿼리</h3>
                  <pre className="smq-test-sql">{result.sql}</pre>
                </div>
                {result.metadata && result.metadata.length > 0 && (
                  <div className="smq-test-result-section">
                    <h3>메타데이터</h3>
                    <pre className="smq-test-metadata">
                      {JSON.stringify(result.metadata, null, 2)}
                    </pre>
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


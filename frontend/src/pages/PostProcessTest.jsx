import React, { useState, useRef, useEffect } from 'react'
import axios from 'axios'
import EditableTable from '../components/EditableTable'
import './PostProcessTest.css'

function PostProcessTest() {
  const [activeTab, setActiveTab] = useState('test') // 'test' or 'prompt'
  const [task, setTask] = useState('')
  const [tableName, setTableName] = useState('result1')
  const [tableColumns, setTableColumns] = useState([
    { key: 'col1', label: '컬럼1' },
    { key: 'col2', label: '컬럼2' },
    { key: 'col3', label: '컬럼3' }
  ])
  const [tableData, setTableData] = useState([])
  const [userQuestion, setUserQuestion] = useState('')
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState(null)
  const [error, setError] = useState(null)
  const [sqlResult, setSqlResult] = useState(null)
  const [executingSql, setExecutingSql] = useState(false)
  const resultEndRef = useRef(null)
  const tableDataRef = useRef(tableData)
  const executedResultRef = useRef(null) // 이미 실행된 result 추적
  
  // 프롬프트 관리 상태
  const [promptContent, setPromptContent] = useState('')
  const [promptLoading, setPromptLoading] = useState(false)
  const [promptSaving, setPromptSaving] = useState(false)
  
  // LLM 설정 상태
  const [llmProvider, setLlmProvider] = useState('devstral') // 'gpt' or 'devstral'
  const [llmConfig, setLlmConfig] = useState({
    url: 'http://183.102.124.135:8001/',
    model_name: '/home/daquv/.cache/huggingface/hub/models--unsloth--Devstral-Small-2507-unsloth-bnb-4bit/snapshots/0578b9b52309df8ae455eb860a6cebe50dc891cd',
    model_type: 'vllm',
    temperature: 0.1,
    max_tokens: 1000
  })

  // tableData 변경 시 ref 업데이트
  useEffect(() => {
    tableDataRef.current = tableData
  }, [tableData])

  useEffect(() => {
    scrollToBottom()
  }, [result])

  // 결과가 생성되고 'pass'가 아니면 자동으로 SQL 실행 (한 번만)
  useEffect(() => {
    // 이미 실행한 result이면 스킵
    if (executedResultRef.current === result) {
      return
    }
    
    if (result && !loading && !executingSql && typeof result === 'string' && result.trim().toLowerCase() !== 'pass') {
      // 실행 표시
      executedResultRef.current = result
      
      // 약간의 지연 후 SQL 실행 (UI 업데이트를 위해)
      const timer = setTimeout(() => {
        // 최신 tableData를 사용하여 SQL 실행
        handleExecuteSQL(result)
      }, 100)
      return () => clearTimeout(timer)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [result, loading, executingSql])

  // 프롬프트 로드
  const loadPrompt = async () => {
    setPromptLoading(true)
    try {
      const response = await axios.get('/api/prompt?prompt_type=postprocess')
      if (response.data.success !== false) {
        setPromptContent(response.data.prompt || '')
      } else {
        console.error('프롬프트 로드 실패: success가 false')
        setPromptContent('')
      }
    } catch (error) {
      console.error('프롬프트 로드 실패:', error)
      alert('프롬프트 로드 실패: ' + (error.response?.data?.detail || error.message))
      setPromptContent('')
    } finally {
      setPromptLoading(false)
    }
  }

  // 프롬프트 저장
  const savePrompt = async () => {
    setPromptSaving(true)
    try {
      const response = await axios.post('/api/prompt', { 
        prompt: promptContent,
        prompt_type: 'postprocess'
      })
      if (response.data.success) {
        alert('프롬프트가 저장되었습니다.')
      }
    } catch (error) {
      console.error('프롬프트 저장 실패:', error)
      alert('프롬프트 저장 실패: ' + (error.response?.data?.detail || error.message))
    } finally {
      setPromptSaving(false)
    }
  }

  // 컴포넌트 마운트 시 프롬프트 탭이면 프롬프트 로드
  useEffect(() => {
    if (activeTab === 'prompt') {
      loadPrompt()
    }
  }, [activeTab])

  const scrollToBottom = () => {
    resultEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }

  // 표 데이터를 마크다운 형식으로 변환
  const convertTableToMarkdown = () => {
    if (tableData.length === 0) return ''
    
    let markdown = ''
    if (task.trim()) {
      markdown += `task: ${task}\n`
    }
    if (tableName.trim()) {
      markdown += `Table Name: ${tableName}\n`
    }
    
    // 실제 컬럼명(키) 목록 명시
    const actualColumnNames = tableColumns.map(col => col.key).join(', ')
    markdown += `\n**⚠️ 중요: 실제 컬럼명 목록**\n`
    markdown += `실제 데이터프레임의 컬럼명은 다음과 같습니다: ${actualColumnNames}\n`
    markdown += `SQL 작성 시 반드시 이 컬럼명을 정확히 사용하세요.\n\n`
    
    // 헤더 생성 (label을 표시용으로 사용)
    const headers = tableColumns.map(col => col.label).join(' | ')
    markdown += `| ${headers} |\n`
    markdown += `| ${tableColumns.map(() => '---').join(' | ')} |\n`
    
    // 데이터 행 생성
    tableData.forEach(row => {
      const values = tableColumns.map(col => row[col.key] || '').join(' | ')
      markdown += `| ${values} |\n`
    })
    
    return markdown
  }

  const handleSubmit = async () => {
    if (tableData.length === 0) {
      alert('데이터프레임에 최소 1개 이상의 행을 추가해주세요.')
      return
    }

    if (!userQuestion.trim()) {
      alert('사용자 질문을 입력해주세요.')
      return
    }

    setLoading(true)
    setError(null)
    setResult(null)
    executedResultRef.current = null // 새로운 실행 시작 시 초기화

    try {
      const dataframeResult = convertTableToMarkdown()
      
      const response = await axios.post('/api/postprocess/test', {
        dataframe_result: dataframeResult,
        user_question: userQuestion,
        llm_config: llmProvider === 'devstral' ? llmConfig : null
      })

      if (response.data.success) {
        const resultData = response.data.result
        setResult(resultData)
        // useEffect에서 자동으로 SQL 실행 처리
      } else {
        setError(response.data.error || '처리 중 오류가 발생했습니다.')
      }
    } catch (err) {
      console.error('후처리 테스트 오류:', err)
      if (err.response?.status === 404) {
        setError('API 엔드포인트를 찾을 수 없습니다. 백엔드 서버를 재시작해주세요.')
      } else {
        setError(err.response?.data?.detail || err.response?.data?.error || err.message || '서버 오류가 발생했습니다.')
      }
    } finally {
      setLoading(false)
    }
  }

  const handleClear = () => {
    setTask('')
    setTableName('result1')
    setTableData([])
    setUserQuestion('')
    setResult(null)
    setError(null)
    setSqlResult(null)
    executedResultRef.current = null // 초기화 시 실행 추적도 초기화
  }

  const handleAddColumn = () => {
    const newKey = `col${tableColumns.length + 1}`
    setTableColumns([...tableColumns, { key: newKey, label: `컬럼${tableColumns.length + 1}` }])
    // 기존 데이터에 새 컬럼 추가
    setTableData(tableData.map(row => ({ ...row, [newKey]: '' })))
  }

  const handleDeleteColumn = (columnKey) => {
    if (tableColumns.length <= 1) {
      alert('최소 1개 이상의 컬럼이 필요합니다.')
      return
    }
    setTableColumns(tableColumns.filter(col => col.key !== columnKey))
    setTableData(tableData.map(row => {
      const newRow = { ...row }
      delete newRow[columnKey]
      return newRow
    }))
  }

  // 컬럼명 입력 중에는 label만 업데이트 (focus 유지)
  const handleColumnNameChange = (columnKey, newLabel) => {
    setTableColumns(tableColumns.map(col => 
      col.key === columnKey ? { ...col, label: newLabel } : col
    ))
  }

  // 컬럼명 입력 완료 시 키 변경 처리 (blur 이벤트)
  const handleColumnNameBlur = (columnKey, newLabel) => {
    // 새 키 생성: label을 기반으로 유효한 키 생성 (공백 제거, 소문자 변환, 언더스코어로 특수문자 대체)
    const normalizeKey = (label) => {
      if (!label || !label.trim()) return columnKey // 빈 값이면 기존 키 유지
      return label.trim()
        .toLowerCase()
        .replace(/[^a-z0-9_]/g, '_') // 영문, 숫자, 언더스코어만 허용
        .replace(/_+/g, '_') // 연속된 언더스코어를 하나로
        .replace(/^_|_$/g, '') // 앞뒤 언더스코어 제거
        || columnKey // 결과가 비어있으면 기존 키 유지
    }
    
    const newKey = normalizeKey(newLabel)
    
    // 키가 변경되지 않았으면 아무것도 하지 않음
    if (newKey === columnKey) {
      return
    }
    
    // 키가 변경된 경우: 컬럼과 데이터 모두 업데이트
    // 1. 컬럼 정보 업데이트
    const updatedColumns = tableColumns.map(col => {
      if (col.key === columnKey) {
        return { ...col, key: newKey, label: newLabel }
      }
      // 다른 컬럼의 키와 충돌하는지 확인
      if (col.key === newKey) {
        // 충돌 시 기존 키에 숫자 추가
        let counter = 1
        let uniqueKey = `${newKey}_${counter}`
        while (tableColumns.some(c => c.key === uniqueKey && c.key !== columnKey)) {
          counter++
          uniqueKey = `${newKey}_${counter}`
        }
        return { ...col, key: uniqueKey }
      }
      return col
    })
    setTableColumns(updatedColumns)
    
    // 2. 데이터의 키도 함께 변경
    const updatedData = tableData.map(row => {
      const newRow = { ...row }
      if (newKey !== columnKey) {
        // 키가 변경된 경우
        newRow[newKey] = row[columnKey]
        delete newRow[columnKey]
      }
      return newRow
    })
    setTableData(updatedData)
  }

  const handleAddRow = (newRow) => {
    setTableData([...tableData, newRow])
  }

  const handleDataChange = (newData) => {
    setTableData(newData)
  }

  // SQL에서 마크다운 코드 블록 제거
  const cleanSQL = (sql) => {
    if (!sql) return sql
    let cleaned = sql.trim()
    // ```sql 또는 ``` 로 시작하는 부분 제거
    cleaned = cleaned.replace(/^```sql\s*/i, '')
    cleaned = cleaned.replace(/^```\s*/i, '')
    // 끝의 ``` 제거
    cleaned = cleaned.replace(/\s*```\s*$/i, '')
    return cleaned.trim()
  }

  const handleExecuteSQL = async (sqlToExecute = null) => {
    const sql = sqlToExecute || result
    if (!sql || sql.trim().toLowerCase() === 'pass') {
      if (!sqlToExecute) {
        alert('실행할 SQL이 없습니다. (pass 결과는 실행할 수 없습니다)')
      }
      return
    }

    setExecutingSql(true)
    setSqlResult(null)
    setError(null)

    try {
      const cleanedSQL = cleanSQL(sql)
      // 최신 tableData 사용 (ref를 통해 최신 값 보장)
      const currentTableData = tableDataRef.current
      const response = await axios.post('/api/postprocess/execute', {
        table_name: tableName || 'result1',
        table_data: currentTableData,
        sql: cleanedSQL
      })

      if (response.data.success) {
        setSqlResult({
          columns: response.data.columns,
          rows: response.data.rows
        })
      } else {
        setError(response.data.error || 'SQL 실행 중 오류가 발생했습니다.')
      }
    } catch (err) {
      console.error('SQL 실행 오류:', err)
      if (err.response?.status === 404) {
        setError('SQL 실행 API 엔드포인트를 찾을 수 없습니다. 백엔드 서버를 재시작해주세요.')
      } else {
        setError(err.response?.data?.detail || err.response?.data?.error || err.message || 'SQL 실행 중 오류가 발생했습니다.')
      }
    } finally {
      setExecutingSql(false)
    }
  }

  const loadExample = () => {
    setTask('2024년 12월, 2025년 1월, 2025년 2월의 부점별 예금 잔액(억원)과 월별 합계를 보여줘')
    setTableName('result1')
    setTableColumns([
      { key: 'brn_nm', label: 'brn_nm' },
      { key: 'base_month_dt', label: 'base_month_dt' },
      { key: 'deposit_balance', label: 'deposit_balance' },
      { key: 'loan_balance', label: 'loan_balance' },
      { key: 'customer_cnt', label: 'customer_cnt' }
    ])
    setTableData([
      { brn_nm: '강남지점', base_month_dt: '2024-12-01', deposit_balance: '485000000000', loan_balance: '320000000000', customer_cnt: '12500' },
      { brn_nm: '강남지점', base_month_dt: '2025-01-01', deposit_balance: '492000000000', loan_balance: '318000000000', customer_cnt: '12680' },
      { brn_nm: '강남지점', base_month_dt: '2025-02-01', deposit_balance: '498000000000', loan_balance: '315000000000', customer_cnt: '12850' },
      { brn_nm: '서초지점', base_month_dt: '2024-12-01', deposit_balance: '380000000000', loan_balance: '285000000000', customer_cnt: '9800' },
      { brn_nm: '서초지점', base_month_dt: '2025-01-01', deposit_balance: '385000000000', loan_balance: '283000000000', customer_cnt: '9950' },
      { brn_nm: '서초지점', base_month_dt: '2025-02-01', deposit_balance: '390000000000', loan_balance: '280000000000', customer_cnt: '10100' },
      { brn_nm: '역삼지점', base_month_dt: '2024-12-01', deposit_balance: '320000000000', loan_balance: '240000000000', customer_cnt: '8200' },
      { brn_nm: '역삼지점', base_month_dt: '2025-01-01', deposit_balance: '325000000000', loan_balance: '238000000000', customer_cnt: '8350' },
      { brn_nm: '역삼지점', base_month_dt: '2025-02-01', deposit_balance: '330000000000', loan_balance: '235000000000', customer_cnt: '8500' },
      { brn_nm: '송파지점', base_month_dt: '2024-12-01', deposit_balance: '280000000000', loan_balance: '210000000000', customer_cnt: '7200' },
      { brn_nm: '송파지점', base_month_dt: '2025-01-01', deposit_balance: '285000000000', loan_balance: '208000000000', customer_cnt: '7350' },
      { brn_nm: '송파지점', base_month_dt: '2025-02-01', deposit_balance: '290000000000', loan_balance: '205000000000', customer_cnt: '7500' }
    ])
    setUserQuestion('2024년 12월, 2025년 1월, 2025년 2월의 부점별 예금 잔액(억원)과 월별 합계를 보여줘')
  }

  return (
    <div className="postprocess-test-page">
      <div className="postprocess-test-header">
        <h2>📊 후처리 테스트</h2>
        <p>데이터프레임 결과와 사용자 질문을 입력하여 LLM 후처리 결과를 확인합니다</p>
        <div className="header-controls">
          <div className="llm-tabs">
            <button
              className={`llm-tab ${llmProvider === 'gpt' ? 'active' : ''}`}
              onClick={() => setLlmProvider('gpt')}
              disabled={loading}
            >
              GPT
            </button>
            <button
              className={`llm-tab ${llmProvider === 'devstral' ? 'active' : ''}`}
              onClick={() => setLlmProvider('devstral')}
              disabled={loading}
            >
              Devstral
            </button>
          </div>
        </div>
      </div>

      {/* 탭 메뉴 */}
      <div className="postprocess-test-tabs">
        <button
          className={`tab-button ${activeTab === 'test' ? 'active' : ''}`}
          onClick={() => setActiveTab('test')}
        >
          🧪 테스트
        </button>
        <button
          className={`tab-button ${activeTab === 'prompt' ? 'active' : ''}`}
          onClick={() => setActiveTab('prompt')}
        >
          ⚙️ 프롬프트 관리
        </button>
      </div>

      {activeTab === 'prompt' ? (
        <div className="prompt-management">
          <div className="prompt-editor-container">
            <div className="prompt-editor-header">
              <div className="prompt-type-info">
                <h3>후처리 프롬프트</h3>
                <span className="prompt-file-name">postprocess_prompt.txt</span>
              </div>
              <button onClick={loadPrompt} disabled={promptLoading} className="load-button">
                {promptLoading ? '⏳ 로딩 중...' : '📥 로드'}
              </button>
            </div>
            <textarea
              value={promptContent}
              onChange={(e) => setPromptContent(e.target.value)}
              placeholder="프롬프트 내용을 입력하세요..."
              className="prompt-textarea-full"
            />
            <div className="prompt-editor-footer">
              <button onClick={savePrompt} disabled={promptSaving} className="save-button">
                {promptSaving ? '⏳ 저장 중...' : '💾 저장'}
              </button>
            </div>
          </div>
        </div>
      ) : (
        <div className="postprocess-test-content">
        <div className="postprocess-input-section">
          <div className="input-group">
            <div className="input-label-row">
              <label>
                <span className="label-icon">📋</span>
                데이터프레임 설정
              </label>
              <button
                type="button"
                onClick={loadExample}
                className="example-button"
                disabled={loading}
                title="예시 템플릿 로드"
              >
                📝 예시 로드
              </button>
            </div>
            
            <div className="dataframe-settings">
              <div className="setting-item">
                <label htmlFor="task-input">Task (선택사항):</label>
                <input
                  id="task-input"
                  type="text"
                  value={task}
                  onChange={(e) => setTask(e.target.value)}
                  placeholder="예: 2024년 12월, 2025년 1월, 2025년 2월의 STS스크랩의 소분류 별 중량(톤), 금액(억원)과 월별 합계 까지 보여줘"
                  className="setting-input"
                  disabled={loading}
                />
              </div>
              <div className="setting-item">
                <label htmlFor="table-name-input">Table Name:</label>
                <input
                  id="table-name-input"
                  type="text"
                  value={tableName}
                  onChange={(e) => setTableName(e.target.value)}
                  placeholder="result1"
                  className="setting-input"
                  disabled={loading}
                />
              </div>
            </div>
          </div>

          <div className="input-group">
            <div className="table-header-controls">
              <label>
                <span className="label-icon">📊</span>
                데이터 테이블
              </label>
              <div className="table-controls">
                <button
                  type="button"
                  onClick={handleAddColumn}
                  className="add-column-button"
                  disabled={loading}
                  title="컬럼 추가"
                >
                  ➕ 컬럼 추가
                </button>
              </div>
            </div>
            
            <div className="editable-table-wrapper-custom">
              <EditableTable
                title=""
                columns={tableColumns.map((col) => ({
                  key: col.key,
                  label: (
                    <div className="column-header-cell">
                      <input
                        type="text"
                        value={col.label}
                        onChange={(e) => handleColumnNameChange(col.key, e.target.value)}
                        onBlur={(e) => handleColumnNameBlur(col.key, e.target.value)}
                        onClick={(e) => e.stopPropagation()}
                        onDoubleClick={(e) => {
                          e.stopPropagation()
                          if (tableColumns.length > 1) {
                            if (window.confirm(`"${col.label}" 컬럼을 삭제하시겠습니까?`)) {
                              handleDeleteColumn(col.key)
                            }
                          }
                        }}
                        className="column-name-input"
                        disabled={loading}
                        placeholder="컬럼명"
                      />
                      {tableColumns.length > 1 && (
                        <button
                          type="button"
                          onClick={(e) => {
                            e.stopPropagation()
                            if (window.confirm(`"${col.label}" 컬럼을 삭제하시겠습니까?`)) {
                              handleDeleteColumn(col.key)
                            }
                          }}
                          className="delete-column-button"
                          disabled={loading}
                          title="컬럼 삭제"
                        >
                          ✕
                        </button>
                      )}
                    </div>
                  ),
                  width: 'auto'
                }))}
                data={tableData}
                onDataChange={handleDataChange}
                onAddRow={handleAddRow}
                onDeleteRow={(rowIndex) => {
                  setTableData(tableData.filter((_, idx) => idx !== rowIndex))
                }}
              />
            </div>
          </div>

          <div className="input-group">
            <label htmlFor="question-input">
              <span className="label-icon">❓</span>
              사용자 질문
            </label>
            <div className="input-hint">
              데이터프레임을 어떻게 가공할지 질문을 입력하세요.
            </div>
            <textarea
              id="question-input"
              value={userQuestion}
              onChange={(e) => setUserQuestion(e.target.value)}
              placeholder={`예시:
- 금액을 억원 단위로 변환해주세요
- 월별 합계를 추가해주세요
- 금액 순으로 정렬해주세요
- 상위 5개만 보여주세요`}
              className="question-textarea"
              disabled={loading}
            />
          </div>

          <div className="action-buttons">
            <button
              onClick={handleSubmit}
              disabled={loading || tableData.length === 0 || !userQuestion.trim()}
              className="submit-button"
            >
              {loading ? '⏳ 처리 중...' : '🚀 실행'}
            </button>
            <button
              onClick={handleClear}
              disabled={loading}
              className="clear-button"
            >
              🗑️ 초기화
            </button>
          </div>
        </div>

        <div className="postprocess-result-section">
          <div className="result-header">
            <h3>📝 결과</h3>
            {result && !loading && result.trim().toLowerCase() !== 'pass' && (
              <button
                onClick={handleExecuteSQL}
                disabled={executingSql}
                className="execute-sql-button"
                title="SQL 실행"
              >
                {executingSql ? '⏳ 실행 중...' : '▶️ SQL 실행'}
              </button>
            )}
          </div>
          <div className="result-content">
            {loading && (
              <div className="loading-indicator">
                <div className="spinner"></div>
                <p>LLM이 결과를 생성하는 중입니다...</p>
              </div>
            )}

            {error && (
              <div className="error-message">
                <span className="error-icon">❌</span>
                <div>
                  <strong>오류 발생</strong>
                  <pre>{error}</pre>
                </div>
              </div>
            )}

            {sqlResult && !executingSql && (
              <div className="sql-result-display">
                <h4>SQL 실행 결과</h4>
                <div className="sql-result-table-wrapper">
                  <table className="sql-result-table">
                    <thead>
                      <tr>
                        {sqlResult.columns.map((col, idx) => (
                          <th key={idx}>{col}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {sqlResult.rows.map((row, rowIdx) => (
                        <tr key={rowIdx}>
                          {row.map((cell, cellIdx) => (
                            <td key={cellIdx}>{cell !== null && cell !== undefined ? String(cell) : '-'}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {executingSql && (
              <div className="loading-indicator">
                <div className="spinner"></div>
                <p>SQL을 실행하는 중입니다...</p>
              </div>
            )}

            {result && !loading && (
              <div className="result-display">
                <h4>생성된 SQL / 결과</h4>
                {typeof result === 'string' ? (
                  <pre className="result-text">{result}</pre>
                ) : (
                  <pre className="result-json">{JSON.stringify(result, null, 2)}</pre>
                )}
              </div>
            )}

            {!loading && !error && !result && (
              <div className="result-placeholder">
                <p>왼쪽에서 데이터프레임 결과와 사용자 질문을 입력한 후 실행 버튼을 클릭하세요.</p>
              </div>
            )}

            <div ref={resultEndRef} />
          </div>
        </div>
        </div>
      )}
    </div>
  )
}

export default PostProcessTest

import React, { useState, useRef, useEffect } from 'react'
import axios from 'axios'
import './AgentTest.css'

function AgentTest() {
  const [activeTab, setActiveTab] = useState('chat') // 'chat' or 'prompt'
  const [userInput, setUserInput] = useState('')
  const [conversation, setConversation] = useState([])
  const [loading, setLoading] = useState(false)
  const [smqState, setSmqState] = useState([])
  const [currentStep, setCurrentStep] = useState(null)
  const [websocket, setWebsocket] = useState(null)
  const [wsConnected, setWsConnected] = useState(false)
  const messagesEndRef = useRef(null)
  
  // 프롬프트 관리 상태 (전체)
  const [promptContent, setPromptContent] = useState('')
  const [promptLoading, setPromptLoading] = useState(false)
  const [promptSaving, setPromptSaving] = useState(false)
  
  // 노드 상태 추적
  const [nodeStatuses, setNodeStatuses] = useState({}) // { step: { status: 'running'|'complete'|'error', prompt: '', result: '', toolResult: {} } }
  const [selectedDetail, setSelectedDetail] = useState(null) // 상세보기 팝업에 표시할 데이터
  
  // 노드 이름 맵
  const nodeNameMap = {
    'classifyJoy': '질문 분류',
    'splitQuestion': '질문 분할',
    'modelSelector': '모델 선택',
    'extractMetrics': '메트릭 추출',
    'extractFilters': '필터 추출',
    'extractOrderByAndLimit': '정렬 및 제한 추출',
    'manipulation': 'SMQ 생성',
    'smq2sql': 'SQL 변환',
    'executeQuery': '쿼리 실행',
    'respondent': '응답 생성',
    'complete': '완료'
  }
  
  // 현재 질문의 최종 데이터 저장
  const [currentQueryData, setCurrentQueryData] = useState(null)
  
  // LLM 설정 상태
  const [llmProvider, setLlmProvider] = useState('devstral') // 'gpt' or 'devstral'
  const [llmConfig, setLlmConfig] = useState({
    url: 'http://183.102.124.135:8001/',
    model_name: '/home/daquv/.cache/huggingface/hub/models--unsloth--Devstral-Small-2507-unsloth-bnb-4bit/snapshots/0578b9b52309df8ae455eb860a6cebe50dc891cd',
    model_type: 'vllm',
    temperature: 0.1,
    max_tokens: 1000
  })

  const addMessage = (role, content, toolCall = null, toolResult = null) => {
    setConversation(prev => [...prev, {
      role,
      content,
      toolCall,
      toolResult,
      timestamp: new Date().toLocaleTimeString()
    }])
  }

  // 프롬프트 로드
  const loadPrompt = async () => {
    setPromptLoading(true)
    try {
      const response = await axios.get('/api/smq/prompt')
      if (response.data.success) {
        setPromptContent(response.data.prompt || '')
      } else if (response.data.prompt) {
        // 전체 프롬프트가 직접 반환된 경우
        setPromptContent(response.data.prompt)
      }
    } catch (error) {
      console.error('프롬프트 로드 실패:', error)
      alert('프롬프트 로드 실패: ' + (error.response?.data?.detail || error.message))
    } finally {
      setPromptLoading(false)
    }
  }

  // 프롬프트 저장
  const savePrompt = async () => {
    setPromptSaving(true)
    try {
      const response = await axios.post('/api/smq/prompt', { prompt: promptContent })
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

  // 컴포넌트 마운트 시 WebSocket 연결
  useEffect(() => {
    loadPrompt()

    // WebSocket 연결
    // 개발 환경에서는 직접 백엔드 서버(포트 8000)에 연결
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const host = window.location.hostname
    // 개발 환경에서는 항상 포트 8000으로 직접 연결
    const wsUrl = `${protocol}//${host}:8000/ws/chat`
    
    console.log('WebSocket 연결 시도:', wsUrl)
    const ws = new WebSocket(wsUrl)
    
    ws.onopen = () => {
      console.log('WebSocket 연결됨')
      setWebsocket(ws)
      setWsConnected(true)
    }
    
    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data)
        handleWebSocketMessage(data)
      } catch (error) {
        console.error('WebSocket 메시지 파싱 오류:', error, event.data)
      }
    }
    
    ws.onerror = (error) => {
      console.error('WebSocket 오류:', error)
      setWsConnected(false)
      // 에러 메시지는 onclose에서 처리
    }
    
    ws.onclose = (event) => {
      console.log('WebSocket 연결 종료:', event.code, event.reason)
      setWebsocket(null)
      setWsConnected(false)
      
      // 비정상 종료인 경우 에러 메시지 표시
      if (event.code !== 1000 && event.code !== 1001) {
        addMessage('error', `WebSocket 연결이 종료되었습니다. (코드: ${event.code}) 서버가 실행 중인지 확인해주세요.`)
      }
    }

    return () => {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.close()
      }
    }
  }, [])

  const handleWebSocketMessage = (data) => {
    const { type, content, tool, args, query_result, sql_result, sql_query, smq, step } = data
    
    if (type === 'thought') {
      addMessage('agent', content)
    } else if (type === 'tool_call') {
      addMessage('tool', `🔧 ${tool} 호출`, { tool, args }, null)
      
      // 재질의인 경우 waiting 상태로 설정
      if (tool === 'HumanInTheLoop.reQuestion') {
        setCurrentStep({ tool, args, status: 'waiting' })
      } else {
        setCurrentStep({ tool, args, status: 'calling' })
      }
    } else if (type === 'tool_result') {
      let result
      try {
        result = JSON.parse(content)
      } catch {
        result = content
      }
      
      // 재질의 대기 상태인 경우
      if (result.status === 'waiting_for_user') {
        setCurrentStep({ tool: 'HumanInTheLoop.reQuestion', args, status: 'waiting' })
        addMessage('tool', `❓ 재질의: ${result.message || content}`, { tool: 'HumanInTheLoop.reQuestion', args }, result)
      } else {
        // SMQ State 업데이트
        if (result.smqState) {
          setSmqState(result.smqState)
        }
        
        addMessage('tool', `✅ ${currentStep?.tool || 'Tool'} 결과`, null, result)
        setCurrentStep(prev => prev ? { ...prev, status: 'complete' } : null)
      }
    } else if (type === 'error') {
      addMessage('error', content)
      setCurrentStep(null)
      setLoading(false)
    } else if (type === 'success') {
      // respondent 노드의 success 이벤트에는 query_result 등이 포함될 수 있음
      if (step === 'respondent' && (query_result || sql_result || sql_query || smq)) {
        addMessage('agent', content, null, {
          query_result: query_result,  // executeQuery에서 생성한 예시 데이터
          sql_result: sql_result,  // SQL 변환 결과
          sql_query: sql_query,  // 생성된 SQL 쿼리
          smq: smq  // 생성된 SMQ
        })
      } else {
        addMessage('agent', content)
      }
      setLoading(false)
      setCurrentStep(null)
    } else if (type === 'message') {
      addMessage('agent', content)
    } else if (type === 'complete') {
      // complete 이벤트에서 최종 답변과 함께 추가 데이터 저장
      addMessage('agent', content, null, {
        query_result: query_result,  // executeQuery에서 생성한 예시 데이터
        sql_result: sql_result,  // SQL 변환 결과
        sql_query: sql_query,  // 생성된 SQL 쿼리
        smq: smq  // 생성된 SMQ
      })
      setLoading(false)
      setCurrentStep(null)
    }
  }

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }

  useEffect(() => {
    scrollToBottom()
  }, [conversation])

  const handleToolCall = async (toolName, args) => {
    try {
      setCurrentStep({ tool: toolName, args, status: 'calling' })
      
      let result = null
      
      if (toolName === 'SemanticModelSelector.selectSemanticModelFiles') {
        // 사용자 질문을 기반으로 시멘틱 모델 파일 선택
        const response = await axios.post('/api/semantic/select-models', {
          userQuery: args.userQuery
        })
        result = response.data
        const selectedNames = result.selected_files.map(f => f.model_name).join(', ')
        addMessage('tool', `📁 시멘틱 모델 파일 선택: ${selectedNames}`, { tool: toolName, args }, result)
        return result
      }
      
      if (toolName === 'SemanticLayer.searchSemanticModels') {
        const response = await axios.post('/api/semantic/search-models', {
          searchQuery: args.searchQuery
        })
        result = response.data.results
        addMessage('tool', `🔍 시멘틱 모델 검색: "${args.searchQuery}"`, { tool: toolName, args }, result)
        return result
      }
      
      if (toolName === 'SemanticLayer.getModelDataElements') {
        const response = await axios.post('/api/semantic/get-elements', {
          searchQuery: args.searchQuery,
          semanticModel: args.semanticModel
        })
        result = response.data.results
        addMessage('tool', `📊 모델 요소 조회: ${args.semanticModel.join(', ')}`, { tool: toolName, args }, result)
        return result
      }
      
      if (toolName === 'SemanticModelQuery.convertSmqToSql') {
        const response = await axios.post('/api/smq/convert', {
          smq: JSON.stringify(args.smq),
          dialect: 'bigquery'
        })
        result = {
          success: response.data.success,
          sql: response.data.sql,
          error: response.data.error || null
        }
        
        // SMQ 변환 실패 시 에러 메시지 표시
        if (!response.data.success) {
          const errorMessage = response.data.error || 'SMQ 변환에 실패했습니다.'
          addMessage('error', `❌ 실패: ${errorMessage}`, { tool: toolName, args }, result)
        } else {
          addMessage('tool', `🔄 SMQ → SQL 변환`, { tool: toolName, args }, result)
        }
        
        // smqState 업데이트
        const newSmqState = args.smq.map((smq, index) => ({
          smq,
          index,
          smqToSqlResult: result.success ? result.sql : result.error
        }))
        setSmqState(newSmqState)
        
        return { smqState: newSmqState }
      }
      
      if (toolName === 'SemanticModelQuery.editSmq') {
        // SMQ 편집 로직 (간단한 구현)
        addMessage('tool', `✏️ SMQ 편집`, { tool: toolName, args }, { message: 'SMQ 편집 기능 준비 중' })
        return { message: 'SMQ 편집 기능 준비 중' }
      }
      
      if (toolName === 'HumanInTheLoop.reQuestion') {
        addMessage('agent', `❓ ${args.reQuestionMessage}`, null, null)
        setCurrentStep({ tool: toolName, args, status: 'waiting' })
        return { status: 'waiting_for_user' }
      }
      
    } catch (error) {
      // HTTP 에러 또는 네트워크 에러 처리
      let errorMsg = '도구 실행 중 오류가 발생했습니다.'
      
      if (error.response) {
        // 서버에서 응답을 받았지만 에러 상태 코드인 경우
        if (error.response.status === 404) {
          errorMsg = '❌ 실패: API 엔드포인트를 찾을 수 없습니다. 백엔드 서버를 재시작해주세요.'
        } else if (error.response.data) {
          // 백엔드에서 반환한 에러 메시지 사용
          errorMsg = `❌ 실패: ${error.response.data.detail || error.response.data.error || error.message || errorMsg}`
        } else {
          errorMsg = `❌ 실패: ${error.message || errorMsg}`
        }
      } else if (error.request) {
        // 요청은 보냈지만 응답을 받지 못한 경우
        errorMsg = '❌ 실패: 서버에 연결할 수 없습니다. 백엔드 서버가 실행 중인지 확인해주세요.'
      } else {
        // 요청 설정 중 에러가 발생한 경우
        errorMsg = `❌ 실패: ${error.message || errorMsg}`
      }
      
      addMessage('error', errorMsg, { tool: toolName, args }, null)
      setCurrentStep(null)
      return { error: errorMsg }
    } finally {
      setCurrentStep(prev => prev ? { ...prev, status: 'complete' } : null)
    }
  }

  const handleSubmit = async () => {
    if (!userInput.trim() || !websocket || websocket.readyState !== WebSocket.OPEN) {
      if (!websocket || websocket.readyState !== WebSocket.OPEN) {
        addMessage('error', 'WebSocket이 연결되지 않았습니다. 페이지를 새로고침해주세요.')
      }
      return
    }

    const userMessage = userInput.trim()
    setUserInput('')
    addMessage('user', userMessage)
    setLoading(true)

    try {
      // WebSocket을 통해 LLM에 질문 전송 (LangGraph 에이전트 사용)
      websocket.send(JSON.stringify({
        message: userMessage,
        agent_type: 'langgraph',
        llm_config: llmProvider === 'devstral' ? llmConfig : null
      }))
    } catch (error) {
      addMessage('error', `처리 중 오류가 발생했습니다: ${error.message}`)
      setLoading(false)
    }
  }

  const handleReQuestionAnswer = async (answer) => {
    if (!currentStep || currentStep.status !== 'waiting' || !websocket || websocket.readyState !== WebSocket.OPEN) {
      return
    }
    
    addMessage('user', answer)
    setCurrentStep(null)
    
    // 재질의 답변을 WebSocket을 통해 전송
    try {
      websocket.send(JSON.stringify({
        message: answer,
        agent_type: 'langgraph',
        llm_config: llmProvider === 'devstral' ? llmConfig : null
      }))
    } catch (error) {
      addMessage('error', `답변 전송 중 오류: ${error.message}`)
    }
  }

  return (
    <div className="agent-test-page">
      <div className="agent-test-header">
        <h2>🤖 에이전트 테스트</h2>
        <p>시멘틱 모델 기반 쿼리(SMQ) 자동 생성 에이전트</p>
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
          <div className="ws-status">
            <span className={wsConnected ? 'status-connected' : 'status-disconnected'}>
              {wsConnected ? '🟢 연결됨' : '🔴 연결 안 됨'}
            </span>
          </div>
        </div>
      </div>

      {/* 탭 메뉴 */}
      <div className="agent-test-tabs">
        <button
          className={`tab-button ${activeTab === 'chat' ? 'active' : ''}`}
          onClick={() => setActiveTab('chat')}
        >
          💬 채팅
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
          <div className="prompt-management-header">
            <div>
              <h3>프롬프트 관리</h3>
              <p className="prompt-subtitle">전체 프롬프트 내용을 수정할 수 있습니다.</p>
            </div>
            <div className="prompt-management-actions">
              <button onClick={loadPrompt} disabled={promptLoading} className="action-button">
                {promptLoading ? '⏳ 로딩 중...' : '🔄 불러오기'}
              </button>
              <button onClick={savePrompt} disabled={promptSaving} className="save-button">
                {promptSaving ? '⏳ 저장 중...' : '💾 저장'}
              </button>
            </div>
          </div>

          <div className="prompt-editor-container">
            <textarea
              value={promptContent}
              onChange={(e) => setPromptContent(e.target.value)}
              placeholder="프롬프트 내용을 입력하세요..."
              className="prompt-textarea-full"
            />
          </div>
        </div>
      ) : (
        <div className="agent-test-content">
        <div className="agent-test-conversation">
          {conversation.length === 0 && (
            <div className="agent-test-welcome">
              <p>질문을 입력하면 에이전트가 자동으로 필요한 시멘틱 모델 파일을 선택하고 SMQ를 생성합니다.</p>
              <p className="example-queries">
                <strong>예시 질문:</strong><br/>
                • 고객별 거래 건수 통계<br/>
                • 직원 정보 조회<br/>
                • 부점별 고객 수 통계
              </p>
            </div>
          )}
          
          {conversation.map((msg, idx) => (
            <div key={idx} className={`message message-${msg.role}`}>
              <div className="message-header">
                <span className="message-role">
                  {msg.role === 'user' ? '👤 사용자' : 
                   msg.role === 'agent' ? '🤖 에이전트' : 
                   msg.role === 'tool' ? '🔧 툴 호출' : '❌ 오류'}
                </span>
                <span className="message-time">{msg.timestamp}</span>
              </div>
              <div className="message-content">
                {msg.content}
              </div>
              
              {msg.toolCall && (
                <div className="tool-call-details">
                  <details>
                    <summary>툴 호출 상세</summary>
                    <pre>{JSON.stringify(msg.toolCall, null, 2)}</pre>
                  </details>
                </div>
              )}
              
              {msg.toolResult && (
                <div className="tool-result-details">
                  {/* query_result가 있으면 테이블로 표시 */}
                  {msg.toolResult.query_result && (
                    <div className="query-result-section">
                      <details open>
                        <summary><strong>📊 생성된 예시 데이터</strong></summary>
                        {msg.toolResult.query_result.rows && msg.toolResult.query_result.rows.length > 0 ? (
                          <div className="data-table-container">
                            <table className="data-table">
                              <thead>
                                <tr>
                                  {msg.toolResult.query_result.columns && msg.toolResult.query_result.columns.map((col, colIdx) => (
                                    <th key={colIdx}>{col}</th>
                                  ))}
                                </tr>
                              </thead>
                              <tbody>
                                {msg.toolResult.query_result.rows.map((row, rowIdx) => (
                                  <tr key={rowIdx}>
                                    {msg.toolResult.query_result.columns && msg.toolResult.query_result.columns.map((col, colIdx) => (
                                      <td key={colIdx}>{row[col] !== null && row[col] !== undefined ? String(row[col]) : '-'}</td>
                                    ))}
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        ) : (
                          <p>데이터가 없습니다.</p>
                        )}
                      </details>
                    </div>
                  )}
                  
                  {/* sql_query가 있으면 코드 블록으로 표시 */}
                  {msg.toolResult.sql_query && (
                    <div className="sql-query-section">
                      <details>
                        <summary><strong>🔍 생성된 SQL 쿼리</strong></summary>
                        <pre className="sql-code"><code>{msg.toolResult.sql_query}</code></pre>
                      </details>
                    </div>
                  )}
                  
                  {/* smq가 있으면 JSON으로 표시 */}
                  {msg.toolResult.smq && (
                    <div className="smq-section">
                      <details>
                        <summary><strong>📋 생성된 SMQ</strong></summary>
                        <pre className="json-code"><code>{JSON.stringify(msg.toolResult.smq, null, 2)}</code></pre>
                      </details>
                    </div>
                  )}
                  
                  {/* sql_result가 있으면 메타데이터 표시 */}
                  {msg.toolResult.sql_result && (
                    <div className="sql-result-section">
                      <details>
                        <summary><strong>🔧 SQL 변환 결과 (메타데이터)</strong></summary>
                        <pre className="json-code"><code>{JSON.stringify(msg.toolResult.sql_result, null, 2)}</code></pre>
                      </details>
                    </div>
                  )}
                  
                  {/* 기타 toolResult 데이터가 있으면 표시 */}
                  {!msg.toolResult.query_result && !msg.toolResult.sql_query && !msg.toolResult.smq && !msg.toolResult.sql_result && (
                    <details>
                      <summary>툴 결과</summary>
                      <pre>{JSON.stringify(msg.toolResult, null, 2)}</pre>
                    </details>
                  )}
                </div>
              )}
            </div>
          ))}
          
          {currentStep && currentStep.status === 'waiting' && (
            <div className="requestion-input">
              <input
                type="text"
                placeholder="재질의에 답변하세요..."
                onKeyPress={(e) => {
                  if (e.key === 'Enter') {
                    handleReQuestionAnswer(e.target.value)
                    e.target.value = ''
                  }
                }}
                autoFocus
              />
            </div>
          )}
          
          <div ref={messagesEndRef} />
        </div>
        
        <div className="agent-test-sidebar">
          <div className="sidebar-section">
            <h3>SMQ State</h3>
            {smqState.length > 0 ? (
              <div className="smq-state-list">
                {smqState.map((state, idx) => (
                  <div key={idx} className="smq-state-item">
                    <div className="smq-state-header">
                      <strong>SMQ #{state.index}</strong>
                      {state.smqToSqlResult && !state.smqToSqlResult.startsWith('Error') && (
                        <span className="status-success">✓ 성공</span>
                      )}
                      {state.smqToSqlResult && state.smqToSqlResult.startsWith('Error') && (
                        <span className="status-error">✗ 오류</span>
                      )}
                    </div>
                    <details>
                      <summary>SMQ 내용</summary>
                      <pre>{JSON.stringify(state.smq, null, 2)}</pre>
                    </details>
                    {state.smqToSqlResult && (
                      <details>
                        <summary>{state.smqToSqlResult.startsWith('Error') ? '오류 메시지' : 'SQL 쿼리'}</summary>
                        <pre className={state.smqToSqlResult.startsWith('Error') ? 'error' : 'sql'}>{state.smqToSqlResult}</pre>
                      </details>
                    )}
                  </div>
                ))}
              </div>
            ) : (
              <p className="empty-state">아직 생성된 SMQ가 없습니다.</p>
            )}
          </div>
        </div>
        </div>
        )}
      
      {activeTab === 'chat' && (
        <div className="agent-test-input">
          <textarea
            value={userInput}
            onChange={(e) => setUserInput(e.target.value)}
            onKeyPress={(e) => {
              if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault()
                handleSubmit()
              }
            }}
            placeholder="질문을 입력하세요... (Enter: 전송, Shift+Enter: 줄바꿈)"
            disabled={loading || (currentStep && currentStep.status === 'waiting')}
          />
          <button
            onClick={handleSubmit}
            disabled={loading || !userInput.trim() || (currentStep && currentStep.status === 'waiting')}
          >
            {loading ? '처리 중...' : '전송'}
          </button>
        </div>
      )}
    </div>
  )
}

export default AgentTest

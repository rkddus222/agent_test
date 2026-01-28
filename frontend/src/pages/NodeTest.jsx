import React, { useState, useRef, useEffect } from 'react'
import axios from 'axios'
import './NodeTest.css'

// 비교 결과 표시 컴포넌트
function CompareResultDisplay({ displayedNodeStatuses, nodeNameMap, nodeOrder }) {
  const completeStatus = displayedNodeStatuses['complete']
  const isComplete = completeStatus && completeStatus.status === 'complete'
  
  // 모든 노드를 실행 순서대로 정렬
  // complete 상태가 있으면 respondent 노드는 제외 (최종 결과에 포함되므로)
  const allNodes = Object.keys(displayedNodeStatuses)
    .filter(step => {
      if (step === 'complete') return false
      // complete 상태가 있으면 respondent 노드는 표시하지 않음
      if (isComplete && step === 'respondent') return false
      return true
    })
    .sort((a, b) => {
      const indexA = nodeOrder.indexOf(a)
      const indexB = nodeOrder.indexOf(b)
      if (indexA === -1 && indexB === -1) return 0
      if (indexA === -1) return 1
      if (indexB === -1) return -1
      return indexA - indexB
    })
  
  return (
    <div className="compare-result-display">
      {/* 최종 결과를 최상단에 표시 */}
      {isComplete && (
        <div className="compare-final-result">
          <h5>최종 결과</h5>
          {completeStatus.result && (
            <div className="compare-result-text">{completeStatus.result}</div>
          )}
          {completeStatus.toolResult && (
            <div className="compare-result-data">
              {/* 1. 생성된 예시 데이터 (기본 펼침) */}
              {completeStatus.toolResult.query_result && (
                <div className="compare-query-result">
                  <details open>
                    <summary><strong>📊 생성된 예시 데이터</strong></summary>
                    {completeStatus.toolResult.query_result.rows && completeStatus.toolResult.query_result.rows.length > 0 ? (
                      <div className="data-table-container">
                        <table className="data-table">
                          <thead>
                            <tr>
                              {completeStatus.toolResult.query_result.columns && completeStatus.toolResult.query_result.columns.map((col, colIdx) => (
                                <th key={colIdx}>{col}</th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {completeStatus.toolResult.query_result.rows.map((row, rowIdx) => (
                              <tr key={rowIdx}>
                                {completeStatus.toolResult.query_result.columns && completeStatus.toolResult.query_result.columns.map((col, colIdx) => (
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
              {/* 2. 생성된 SMQ (기본 펼침) */}
              {completeStatus.toolResult.smq && (
                <div className="compare-smq">
                  <details open>
                    <summary><strong>📋 생성된 SMQ</strong></summary>
                    <pre className="json-code"><code>{JSON.stringify(completeStatus.toolResult.smq, null, 2)}</code></pre>
                  </details>
                </div>
              )}
              {/* 3. 생성된 SQL 쿼리 */}
              {completeStatus.toolResult.sql_query && (
                <div className="compare-sql-query">
                  <details>
                    <summary><strong>🔍 생성된 SQL 쿼리</strong></summary>
                    <pre className="sql-code"><code>{completeStatus.toolResult.sql_query}</code></pre>
                  </details>
                </div>
              )}
            </div>
          )}
        </div>
      )}
      
      {/* 모든 노드 상태 표시 */}
      {allNodes.map(step => {
        const nodeStatus = displayedNodeStatuses[step]
        const nodeName = nodeNameMap[step] || step
        
        if (nodeStatus?.status === 'running') {
          return (
            <div key={step} className="compare-node-item compare-node-running">
              <div className="compare-node-icon">🔄</div>
              <div className="compare-node-info">
                <div className="compare-node-name">{nodeName}</div>
                <div className="compare-node-status">실행 중...</div>
              </div>
            </div>
          )
        } else if (nodeStatus?.status === 'complete') {
          return (
            <div key={step} className="compare-node-item compare-node-complete">
              <div className="compare-node-icon">✅</div>
              <div className="compare-node-info">
                <div className="compare-node-name">{nodeName}</div>
                {nodeStatus.result && (
                  <div className="compare-node-result">
                    {step === 'postprocess' && nodeStatus.postprocess_result ? (
                      <pre className="compare-node-sql">{nodeStatus.postprocess_result}</pre>
                    ) : (
                      <div className="compare-node-result-text">{String(nodeStatus.result).substring(0, 100)}...</div>
                    )}
                  </div>
                )}
              </div>
            </div>
          )
        } else if (nodeStatus?.status === 'error') {
          return (
            <div key={step} className="compare-node-item compare-node-error">
              <div className="compare-node-icon">❌</div>
              <div className="compare-node-info">
                <div className="compare-node-name">{nodeName}</div>
                <div className="compare-node-error-text">{nodeStatus.result}</div>
              </div>
            </div>
          )
        }
        return null
      })}
    </div>
  )
}

function NodeTest() {
  const [activeTab, setActiveTab] = useState('chat') // 'chat', 'prompt', or 'compare'
  const [userInput, setUserInput] = useState('')
  const [conversation, setConversation] = useState([])
  const [loading, setLoading] = useState(false)
  
  // 비교 테스트 상태
  const [compareRunning, setCompareRunning] = useState(false)
  const [gptResult, setGptResult] = useState(null) // { nodeStatuses, displayedNodeStatuses, finalResponse, ... }
  const [devstralResult, setDevstralResult] = useState(null)
  const [compareUserInput, setCompareUserInput] = useState('')
  const [gptWebsocket, setGptWebsocket] = useState(null)
  const [devstralWebsocket, setDevstralWebsocket] = useState(null)
  const [nodes, setNodes] = useState([
    { 
      id: 1, 
      name: 'LangGraph 워크플로우', 
      agentType: 'langgraph', 
      promptType: 'langgraph',
      promptContent: '',
      status: 'idle', 
      result: null 
    }
  ])
  const [websocket, setWebsocket] = useState(null)
  const [wsConnected, setWsConnected] = useState(false)
  const [running, setRunning] = useState(false)
  const [cancelled, setCancelled] = useState(false)
  const messagesEndRef = useRef(null)
  const currentMessageHandlers = useRef([])
  const currentTimeouts = useRef([])
  
  // 노드 실행 상태 추적
  const [nodeStatuses, setNodeStatuses] = useState({}) // 백엔드에서 받은 실제 상태 (상세보기용)
  const [displayedNodeStatuses, setDisplayedNodeStatuses] = useState({}) // UI에 표시되는 상태 (Visual Queue 처리 후)
  const [selectedNodeDetail, setSelectedNodeDetail] = useState(null) // 팝업에 표시할 노드 정보
  
  // Visual Queue: 백엔드 이벤트를 큐에 저장하고 순차적으로 표시
  const visualQueueRef = useRef([]) // { step, eventType, data, timestamp }[]
  const processingRef = useRef(false) // 현재 큐 처리 중인지 여부
  const displayTimerRef = useRef(null) // 현재 표시 중인 노드의 타이머
  const [visualQueueLength, setVisualQueueLength] = useState(0) // 큐 길이 (리렌더링 트리거용)
  const completeNodeTimersRef = useRef({}) // 완료된 노드 제거 타이머 { step: timer }
  
  // 프롬프트 관리 상태
  const [selectedPromptType, setSelectedPromptType] = useState('classify_joy')
  const [promptLoading, setPromptLoading] = useState(false)
  const [promptSaving, setPromptSaving] = useState(false)
  
  // LangGraph 노드 프롬프트 타입 목록
  const promptTypes = [
    { value: 'classify_joy', label: '질문 분류', file: 'classify_joy_prompt.txt' },
    { value: 'split_question', label: '질문 분할', file: 'split_question_prompt.txt' },
    { value: 'entity_selector', label: 'Entity 선택', file: 'entity_selector_prompt.txt' },
    { value: 'extract_metrics', label: 'Metrics 추출', file: 'extract_metrics_prompt.txt' },
    { value: 'extract_filters', label: 'Filters 추출', file: 'extract_filters_prompt.txt' },
    { value: 'extract_order_by_and_limit', label: 'Order by & Limit 추출', file: 'extract_order_by_and_limit_prompt.txt' },
    { value: 'postprocess', label: '후처리', file: 'postprocess_prompt.txt' }
  ]
  
  // 노드 이름 매핑
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
    'postprocess': '후처리',
    'respondent': '응답 생성',
    'complete': '완료'
  }
  
  // 노드 실행 순서
  const nodeOrder = [
    'classifyJoy',
    'splitQuestion',
    'modelSelector',
    'extractMetrics',
    'extractFilters',
    'extractOrderByAndLimit',
    'manipulation',
    'smq2sql',
    'executeQuery',
    'postprocess',
    'respondent',
    'complete'
  ]
  
  // 모든 프롬프트 타입 목록 (노드 설정용)
  const allPromptTypes = [
    { value: 'test', label: '테스트 (SemanticAgent)', agentType: 'semantic' },
    { value: 'smq', label: 'SMQ (SMQAgent)', agentType: 'smq' },
    ...promptTypes.map(pt => ({ ...pt, agentType: 'langgraph' }))
  ]
  
  const [promptContent, setPromptContent] = useState('')
  const promptTextareaRef = useRef(null)
  
  // LLM 설정 상태
  const [llmProvider, setLlmProvider] = useState('devstral') // 'gpt' or 'devstral'
  const [llmConfig, setLlmConfig] = useState({
    url: 'http://183.102.124.135:8001/',
    model_name: '/home/daquv/.cache/huggingface/hub/models--unsloth--Devstral-Small-2507-unsloth-bnb-4bit/snapshots/0578b9b52309df8ae455eb860a6cebe50dc891cd',
    model_type: 'vllm',
    temperature: 0.1,
    max_tokens: 1000
  })

  // 프롬프트 로드
  const loadPrompt = async (promptType = selectedPromptType) => {
    setPromptLoading(true)
    try {
      const response = await axios.get(`/api/prompt?prompt_type=${promptType}`)
      console.log('프롬프트 로드 응답:', response.data)
      // success가 있으면 success를 확인하고, 없으면 기존 형식(prompt만 있는 경우)도 지원
      if (response.data.success !== false) {
        setPromptContent(response.data.prompt || '')
      } else {
        console.error('프롬프트 로드 실패: success가 false')
        setPromptContent('')
      }
    } catch (error) {
      console.error('프롬프트 로드 실패:', error)
      console.error('에러 상세:', error.response?.data)
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
        prompt_type: selectedPromptType
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
  
  // 프롬프트 타입 변경 핸들러
  const handlePromptTypeChange = (promptType) => {
    setSelectedPromptType(promptType)
    loadPrompt(promptType)
  }
  
  // 컴포넌트 마운트 시 첫 번째 프롬프트 로드
  useEffect(() => {
    if (activeTab === 'prompt') {
      loadPrompt(selectedPromptType)
    }
  }, [activeTab])

  // textarea 높이 자동 조절
  useEffect(() => {
    const textarea = promptTextareaRef.current
    if (textarea) {
      // 높이를 초기화하고 내용에 맞게 조절
      textarea.style.height = 'auto'
      textarea.style.height = `${textarea.scrollHeight}px`
    }
  }, [promptContent, selectedPromptType])

  // textarea 높이 조절 함수
  const handleTextareaChange = (e) => {
    setPromptContent(e.target.value)
    const textarea = e.target
    // 높이를 초기화하고 내용에 맞게 조절
    textarea.style.height = 'auto'
    textarea.style.height = `${textarea.scrollHeight}px`
  }

  // WebSocket 연결
  useEffect(() => {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const host = window.location.hostname
    const wsUrl = `${protocol}//${host}:8000/ws/chat`
    
    console.log('WebSocket 연결 시도:', wsUrl)
    const ws = new WebSocket(wsUrl)
    
    ws.onopen = () => {
      console.log('WebSocket 연결됨')
      setWebsocket(ws)
      setWsConnected(true)
    }
    
    ws.onerror = (error) => {
      console.error('WebSocket 오류:', error)
      setWsConnected(false)
    }
    
    ws.onclose = (event) => {
      console.log('WebSocket 연결 종료:', event.code, event.reason)
      setWebsocket(null)
      setWsConnected(false)
    }

    return () => {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.close()
      }
    }
  }, [])
  
  // 비교 테스트용 WebSocket 연결 (GPT와 Devstral 각각)
  useEffect(() => {
    if (activeTab !== 'compare') {
      // 비교 테스트 탭이 아니면 연결 정리
      if (gptWebsocket && gptWebsocket.readyState === WebSocket.OPEN) {
        gptWebsocket.close()
      }
      if (devstralWebsocket && devstralWebsocket.readyState === WebSocket.OPEN) {
        devstralWebsocket.close()
      }
      return
    }
    
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const host = window.location.hostname
    const wsUrl = `${protocol}//${host}:8000/ws/chat`
    
    // GPT WebSocket
    const gptWs = new WebSocket(wsUrl)
    gptWs.onopen = () => {
      console.log('GPT WebSocket 연결됨')
      setGptWebsocket(gptWs)
    }
    gptWs.onerror = (error) => {
      console.error('GPT WebSocket 오류:', error)
    }
    gptWs.onclose = () => {
      console.log('GPT WebSocket 연결 종료')
      setGptWebsocket(null)
    }
    
    // Devstral WebSocket
    const devstralWs = new WebSocket(wsUrl)
    devstralWs.onopen = () => {
      console.log('Devstral WebSocket 연결됨')
      setDevstralWebsocket(devstralWs)
    }
    devstralWs.onerror = (error) => {
      console.error('Devstral WebSocket 오류:', error)
    }
    devstralWs.onclose = () => {
      console.log('Devstral WebSocket 연결 종료')
      setDevstralWebsocket(null)
    }
    
    return () => {
      if (gptWs && gptWs.readyState === WebSocket.OPEN) {
        gptWs.close()
      }
      if (devstralWs && devstralWs.readyState === WebSocket.OPEN) {
        devstralWs.close()
      }
    }
  }, [activeTab])

  useEffect(() => {
    scrollToBottom()
  }, [conversation])

  // Visual Queue 처리: 큐에 있는 이벤트를 순차적으로 UI에 반영
  useEffect(() => {
    const processQueue = () => {
      // 이미 처리 중이거나 큐가 비어있으면 중단
      if (processingRef.current || visualQueueRef.current.length === 0) {
        return
      }

      processingRef.current = true
      const queueItem = visualQueueRef.current.shift()
      setVisualQueueLength(visualQueueRef.current.length) // 큐 길이 업데이트
      
      if (!queueItem) {
        processingRef.current = false
        return
      }

      const { step, eventType, data } = queueItem

      // prompt 이벤트: running 상태로 표시 시작
      if (eventType === 'prompt') {
        // 이전에 완료된 노드들의 타이머를 취소
        Object.keys(displayedNodeStatuses).forEach(key => {
          if (key !== 'complete' && key !== step && displayedNodeStatuses[key]?.status === 'complete') {
            if (completeNodeTimersRef.current[key]) {
              clearTimeout(completeNodeTimersRef.current[key])
              delete completeNodeTimersRef.current[key]
            }
          }
        })
        
        // 새 노드를 running 상태로 추가
        setDisplayedNodeStatuses(prev => ({
          ...prev,
          [step]: {
            ...prev[step],
            status: 'running',
            prompt: data.content
          }
        }))
        
        // 이전 완료된 노드들을 페이드아웃 후 제거
        const completedKeys = Object.keys(displayedNodeStatuses).filter(key => 
          key !== 'complete' && key !== step && displayedNodeStatuses[key]?.status === 'complete'
        )
        
        if (completedKeys.length > 0) {
          // 먼저 opacity를 0으로 설정 (fade out 시작)
          completedKeys.forEach(key => {
            const nodeElement = document.querySelector(`[data-node-step="${key}"]`)
            if (nodeElement) {
              nodeElement.style.opacity = '0'
            }
          })
          
          // 애니메이션 시간 후 실제 제거
          setTimeout(() => {
            setDisplayedNodeStatuses(prev => {
              const updated = { ...prev }
              completedKeys.forEach(key => {
                delete updated[key]
              })
              return updated
            })
          }, 300) // 0.3초 후 제거 (transition 시간)
        }
        
        // 1초 후 다음 큐 항목 처리
        if (displayTimerRef.current) {
          clearTimeout(displayTimerRef.current)
        }
        displayTimerRef.current = setTimeout(() => {
          processingRef.current = false
          processQueue()
        }, 1000)
      }
      // thought, tool_result, success 이벤트: complete 상태로 변경
      else if (eventType === 'thought' || eventType === 'tool_result' || eventType === 'success') {
        // 이전 타이머가 있으면 정리
        if (displayTimerRef.current) {
          clearTimeout(displayTimerRef.current)
          displayTimerRef.current = null
        }
        
        // 이전 노드가 running 상태였으면 complete로 변경
        setDisplayedNodeStatuses(prev => {
          const currentStatus = prev[step]?.status
          if (currentStatus === 'running') {
            // running 상태였으면 complete로 변경하고 바로 다음 큐 항목 처리
            setTimeout(() => {
              processingRef.current = false
              processQueue()
            }, 50) // 짧은 딜레이로 상태 업데이트 후 처리
            
            // 완료된 노드를 다음 노드가 시작될 때까지 유지
            // (다음 노드의 prompt 이벤트에서 제거됨)
            // 만약 다음 노드가 없을 경우를 대비해 긴 시간 후 자동 제거
            if (completeNodeTimersRef.current[step]) {
              clearTimeout(completeNodeTimersRef.current[step])
            }
            completeNodeTimersRef.current[step] = setTimeout(() => {
              // 다음 노드가 아직 시작되지 않았을 때만 제거
              setDisplayedNodeStatuses(prevStatuses => {
                // 다음 노드가 이미 실행 중이면 제거하지 않음 (다음 노드에서 처리)
                const hasNextRunning = Object.keys(prevStatuses).some(key => 
                  key !== step && key !== 'complete' && prevStatuses[key]?.status === 'running'
                )
                
                if (hasNextRunning) {
                  // 다음 노드가 이미 시작되었으므로 제거하지 않음
                  return prevStatuses
                }
                
                const updated = { ...prevStatuses }
                delete updated[step]
                return updated
              })
              delete completeNodeTimersRef.current[step]
            }, 10000) // 10초 후 제거 (안전장치)
            
            return {
              ...prev,
              [step]: {
                ...prev[step],
                status: 'complete',
                result: data.content,
                toolResult: data.toolResult || prev[step]?.toolResult,
                details: data.details || prev[step]?.details || null,  // details 저장
                postprocess_result: data.postprocess_result || prev[step]?.postprocess_result || null  // postprocess 결과 저장
              }
            }
          }
          // running 상태가 아니면 (아직 표시 안 됨) 바로 다음 항목 처리
          setTimeout(() => {
            processingRef.current = false
            processQueue()
          }, 0)
          return prev
        })
      }
      // error 이벤트: error 상태로 변경
      else if (eventType === 'error') {
        if (displayTimerRef.current) {
          clearTimeout(displayTimerRef.current)
          displayTimerRef.current = null
        }
        setDisplayedNodeStatuses(prev => ({
          ...prev,
          [step]: {
            ...prev[step],
            status: 'error',
            result: data.content
          }
        }))
        
        // 에러 노드도 3초 후 자동으로 제거
        if (completeNodeTimersRef.current[step]) {
          clearTimeout(completeNodeTimersRef.current[step])
        }
        completeNodeTimersRef.current[step] = setTimeout(() => {
          setDisplayedNodeStatuses(prevStatuses => {
            const updated = { ...prevStatuses }
            delete updated[step]
            return updated
          })
          delete completeNodeTimersRef.current[step]
        }, 3000) // 3초 후 제거
        
        processingRef.current = false
        setTimeout(() => processQueue(), 0)
      }
      // complete 이벤트: complete 노드 업데이트 및 모든 running 노드 완료 처리
      else if (eventType === 'complete') {
        if (displayTimerRef.current) {
          clearTimeout(displayTimerRef.current)
          displayTimerRef.current = null
        }
        
        // 모든 running 상태의 노드를 complete로 변경
        setDisplayedNodeStatuses(prev => {
          const updated = { ...prev }
          
          // complete 노드 업데이트
          updated['complete'] = {
            status: 'complete',
            result: data.content,
            toolResult: data.toolResult
          }
          
          // 모든 running 상태의 노드를 complete로 변경하고 자동 제거 타이머 설정
          Object.keys(updated).forEach(key => {
            if (key !== 'complete' && updated[key]?.status === 'running') {
              updated[key] = {
                ...updated[key],
                status: 'complete'
              }
              
              // 완료된 노드는 다음 노드가 시작될 때 제거됨
              // (안전장치로 긴 시간 후 자동 제거)
              if (completeNodeTimersRef.current[key]) {
                clearTimeout(completeNodeTimersRef.current[key])
              }
              completeNodeTimersRef.current[key] = setTimeout(() => {
                setDisplayedNodeStatuses(prevStatuses => {
                  const updatedStatuses = { ...prevStatuses }
                  delete updatedStatuses[key]
                  return updatedStatuses
                })
                delete completeNodeTimersRef.current[key]
              }, 10000) // 10초 후 제거 (안전장치)
            }
          })
          
          return updated
        })
        
        processingRef.current = false
      }
    }

    processQueue()
  }, [visualQueueLength]) // 큐 길이 변경 시마다 처리

  // 컴포넌트 언마운트 시 타이머 정리
  useEffect(() => {
    return () => {
      if (displayTimerRef.current) {
        clearTimeout(displayTimerRef.current)
      }
      // 모든 완료 노드 타이머 정리
      Object.values(completeNodeTimersRef.current).forEach(timer => {
        clearTimeout(timer)
      })
      completeNodeTimersRef.current = {}
    }
  }, [])

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }

  const addMessage = (role, content, toolCall = null, toolResult = null, details = null, step = null) => {
    setConversation(prev => [...prev, {
      role,
      content,
      toolCall,
      toolResult,
      details,
      step,
      timestamp: new Date().toLocaleTimeString()
    }])
  }

  // 노드 추가 (LangGraph는 단일 워크플로우이므로 노드 추가 불필요)
  const addNode = () => {
    // LangGraph 에이전트는 단일 워크플로우이므로 노드 추가 불필요
    alert('LangGraph 에이전트는 단일 워크플로우로 실행됩니다.')
  }

  // 노드 삭제 (LangGraph는 단일 워크플로우이므로 노드 삭제 불필요)
  const removeNode = (nodeId) => {
    // LangGraph 에이전트는 단일 워크플로우이므로 노드 삭제 불필요
    alert('LangGraph 에이전트는 단일 워크플로우로 실행됩니다.')
  }

  // 노드 업데이트
  const updateNode = (nodeId, updates) => {
    setNodes(nodes.map(n => n.id === nodeId ? { ...n, ...updates } : n))
  }

  // 노드 실행 (단일)
  const runNode = async (node, inputMessage) => {
    if (!websocket || websocket.readyState !== WebSocket.OPEN) {
      throw new Error('WebSocket이 연결되지 않았습니다.')
    }

    // 취소 상태 확인
    if (cancelled) {
      throw new Error('작업이 취소되었습니다.')
    }

    updateNode(node.id, { status: 'running', result: null })

    return new Promise((resolve, reject) => {
      let nodeResult = null
      let isResolved = false
      
      const timeout = setTimeout(() => {
        if (!isResolved) {
          websocket.removeEventListener('message', messageHandler)
          const errorMessage = '❌ 실패: 요청 시간이 초과되었습니다. (5분)'
          addMessage('error', errorMessage)
          updateNode(node.id, { status: 'error', result: { error: '타임아웃' } })
          isResolved = true
          reject(new Error('타임아웃'))
        }
      }, 300000)
      
      // 타임아웃을 추적하기 위해 저장
      currentTimeouts.current.push(timeout)

      const messageHandler = (event) => {
        try {
          // 취소 상태 확인
          if (cancelled && !isResolved) {
            clearTimeout(timeout)
            websocket.removeEventListener('message', messageHandler)
            updateNode(node.id, { status: 'cancelled', result: { cancelled: true } })
            addMessage('system', `⏹️ ${node.name} 실행 취소됨`)
            isResolved = true
            reject(new Error('작업이 취소되었습니다.'))
            return
          }

          const data = JSON.parse(event.data)
          const { type, content, tool, args, details, step, query_result, sql_result, sql_query, smq, postprocess_result } = data
          // details는 extractMetrics, extractFilters, extractOrderByAndLimit 등에서 추출된 상세 정보를 포함
          

          if (type === 'cancelled') {
            clearTimeout(timeout)
            websocket.removeEventListener('message', messageHandler)
            updateNode(node.id, { status: 'cancelled', result: { cancelled: true } })
            isResolved = true
            reject(new Error('작업이 취소되었습니다.'))
          } else if (type === 'prompt') {
            if (!cancelled && step) {
              // 백엔드 실제 상태 업데이트 (상세보기용)
              setNodeStatuses(prev => ({
                ...prev,
                [step]: {
                  ...prev[step],
                  status: 'running',
                  prompt: content
                }
              }))
              
              // Visual Queue에 추가 (UI 표시용)
              visualQueueRef.current.push({
                step,
                eventType: 'prompt',
                data: { content },
                timestamp: Date.now()
              })
              setVisualQueueLength(visualQueueRef.current.length) // 큐 길이 업데이트로 리렌더링 트리거
            }
          } else if (type === 'thought') {
            // thought는 노드 완료를 나타냄 (상태를 complete로 변경)
            if (!cancelled && step) {
              // postprocess 노드의 경우 postprocess_result를 우선 사용
              const displayContent = (step === 'postprocess' && postprocess_result) ? postprocess_result : content
              
              // 백엔드 실제 상태 업데이트 (details도 함께 저장)
              setNodeStatuses(prev => ({
                ...prev,
                [step]: {
                  ...prev[step],
                  status: 'complete',
                  result: displayContent,
                  details: details || null,  // details 저장 (metrics, filters, order_by 등)
                  postprocess_result: postprocess_result || null  // postprocess 결과 저장
                }
              }))
              
              // Visual Queue에 추가 (details 포함)
              visualQueueRef.current.push({
                step,
                eventType: 'thought',
                data: { content: displayContent, details: details || null, postprocess_result: postprocess_result || null },
                timestamp: Date.now()
              })
              setVisualQueueLength(visualQueueRef.current.length)
            }
          } else if (type === 'tool_call') {
            // tool_call은 상태만 추적 (큐에 추가하지 않음)
            if (!cancelled && step) {
              setNodeStatuses(prev => ({
                ...prev,
                [step]: {
                  ...prev[step],
                  status: prev[step]?.status || 'running'
                }
              }))
            }
          } else if (type === 'tool_result') {
            if (!cancelled && step) {
              let result
              try {
                result = JSON.parse(content)
              } catch {
                result = content
              }
              
              // 백엔드 실제 상태 업데이트
              setNodeStatuses(prev => ({
                ...prev,
                [step]: {
                  ...prev[step],
                  status: 'complete',
                  result: content,
                  toolResult: result
                }
              }))
              
              // Visual Queue에 추가
              visualQueueRef.current.push({
                step,
                eventType: 'tool_result',
                data: { content, toolResult: result },
                timestamp: Date.now()
              })
              setVisualQueueLength(visualQueueRef.current.length)
            }
          } else if (type === 'error') {
            clearTimeout(timeout)
            websocket.removeEventListener('message', messageHandler)
            updateNode(node.id, { status: 'error', result: { error: content } })
            
            // 에러 메시지를 conversation에 추가하여 사용자에게 표시
            const errorMessage = `❌ 실패: ${content}`
            addMessage('error', errorMessage, null, null, null, step)
            
            if (step) {
              setNodeStatuses(prev => ({
                ...prev,
                [step]: {
                  ...prev[step],
                  status: 'error',
                  result: content
                }
              }))
              
              // Visual Queue에 추가
              visualQueueRef.current.push({
                step,
                eventType: 'error',
                data: { content },
                timestamp: Date.now()
              })
              setVisualQueueLength(visualQueueRef.current.length)
            }
            isResolved = true
            reject(new Error(content))
          } else if (type === 'success' || type === 'message') {
            if (!cancelled && step) {
              nodeResult = content
              
              // 백엔드 실제 상태 업데이트
              setNodeStatuses(prev => ({
                ...prev,
                [step]: {
                  ...prev[step],
                  status: 'complete',
                  result: content,
                  toolResult: (query_result || sql_result || sql_query || smq) ? {
                    query_result: query_result,
                    sql_result: sql_result,
                    sql_query: sql_query,
                    smq: smq
                  } : prev[step]?.toolResult
                }
              }))
              
              // Visual Queue에 추가
              visualQueueRef.current.push({
                step,
                eventType: 'success',
                data: {
                  content,
                  toolResult: (query_result || sql_result || sql_query || smq) ? {
                    query_result: query_result,
                    sql_result: sql_result,
                    sql_query: sql_query,
                    smq: smq
                  } : null
                },
                timestamp: Date.now()
              })
              setVisualQueueLength(visualQueueRef.current.length)
            }
          } else if (type === 'complete') {
            if (!cancelled && !isResolved) {
              clearTimeout(timeout)
              websocket.removeEventListener('message', messageHandler)
              const finalResult = nodeResult || content || 'Task completed.'
              
              // 백엔드 실제 상태 업데이트
              setNodeStatuses(prev => ({
                ...prev,
                'complete': {
                  status: 'complete',
                  result: finalResult,
                  toolResult: {
                    query_result: query_result,
                    sql_result: sql_result,
                    sql_query: sql_query,
                    smq: smq
                  }
                }
              }))
              
              // Visual Queue에 추가
              visualQueueRef.current.push({
                step: 'complete',
                eventType: 'complete',
                data: {
                  content: finalResult,
                  toolResult: {
                    query_result: query_result,
                    sql_result: sql_result,
                    sql_query: sql_query,
                    smq: smq
                  }
                },
                timestamp: Date.now()
              })
              setVisualQueueLength(visualQueueRef.current.length)
              
              updateNode(node.id, { 
                status: 'complete', 
                result: { 
                  success: true, 
                  content: finalResult,
                  query_result: query_result,
                  sql_result: sql_result,
                  sql_query: sql_query,
                  smq: smq
                } 
              })
              isResolved = true
              resolve({ 
                success: true, 
                content: finalResult,
                query_result: query_result,
                sql_result: sql_result,
                sql_query: sql_query,
                smq: smq
              })
            }
          }
        } catch (error) {
          console.error('메시지 파싱 오류:', error)
          // 파싱 오류도 사용자에게 알림
          if (!isResolved) {
            const errorMessage = `❌ 실패: 메시지 처리 중 오류가 발생했습니다. (${error.message})`
            addMessage('error', errorMessage)
            updateNode(node.id, { status: 'error', result: { error: error.message } })
            clearTimeout(timeout)
            websocket.removeEventListener('message', messageHandler)
            isResolved = true
            reject(error)
          }
        }
      }

      websocket.addEventListener('message', messageHandler)
      // 메시지 핸들러를 추적하기 위해 저장
      currentMessageHandlers.current.push({ handler: messageHandler, nodeId: node.id })

      // LangGraph 에이전트는 항상 전체 워크플로우를 실행
      // 메시지 전송
      websocket.send(JSON.stringify({
        message: inputMessage,
        agent_type: 'langgraph',
        prompt_type: '', // LangGraph 에이전트는 내부적으로 프롬프트를 관리하므로 prompt_type 불필요
        llm_config: llmProvider === 'devstral' ? llmConfig : null
      }))
    })
  }

  // Flow 실행 (파이프라인 형태로 순차 실행)
  const runFlow = async () => {
    if (!websocket || websocket.readyState !== WebSocket.OPEN) {
      alert('WebSocket이 연결되지 않았습니다.')
      return
    }
    
    // 노드 상태 초기화
    setNodeStatuses({})
    setSelectedNodeDetail(null)

    if (!userInput.trim()) {
      alert('사용자 입력을 입력해주세요.')
      return
    }

    // 모든 노드 초기화
    setNodes(nodes.map(n => ({ ...n, status: 'idle', result: null })))
    setRunning(true)
    setLoading(true)
    setCancelled(false)
    setConversation([])
    
    // Visual Queue 초기화
    visualQueueRef.current = []
    setVisualQueueLength(0)
    setDisplayedNodeStatuses({})
    setNodeStatuses({})
    
    // 타이머 정리
    if (displayTimerRef.current) {
      clearTimeout(displayTimerRef.current)
      displayTimerRef.current = null
    }
    // 모든 완료 노드 타이머 정리
    Object.values(completeNodeTimersRef.current).forEach(timer => {
      clearTimeout(timer)
    })
    completeNodeTimersRef.current = {}
    processingRef.current = false
    
    // 이전 핸들러와 타임아웃 정리
    currentMessageHandlers.current.forEach(({ handler }) => {
      websocket.removeEventListener('message', handler)
    })
    currentTimeouts.current.forEach(timeout => clearTimeout(timeout))
    currentMessageHandlers.current = []
    currentTimeouts.current = []
    
    addMessage('user', userInput)

    try {
      // LangGraph 에이전트 실행 (전체 워크플로우)
      const langgraphNode = nodes[0]
      await runNode(langgraphNode, userInput)
      
    } catch (error) {
      // 에러는 이미 messageHandler에서 처리되므로 여기서는 추가하지 않음
      // (중복 방지)
    } finally {
      setRunning(false)
      setLoading(false)
    }
  }

  // 취소 함수
  const cancelFlow = () => {
    if (!running) return
    
    setCancelled(true)
    addMessage('system', '⏹️ 실행 취소 요청 중...')
    
    // 백엔드에 취소 신호 전송
    if (websocket && websocket.readyState === WebSocket.OPEN) {
      websocket.send(JSON.stringify({
        type: 'cancel'
      }))
    }
    
    // 모든 메시지 핸들러 제거
    currentMessageHandlers.current.forEach(({ handler }) => {
      websocket.removeEventListener('message', handler)
    })
    currentMessageHandlers.current = []
    
    // 모든 타임아웃 정리
    currentTimeouts.current.forEach(timeout => clearTimeout(timeout))
    currentTimeouts.current = []
    
    // 실행 중인 노드들을 취소 상태로 변경
    setNodes(nodes.map(n => 
      n.status === 'running' ? { ...n, status: 'cancelled' } : n
    ))
    
    setRunning(false)
    setLoading(false)
  }

  const handleSubmit = async () => {
    if (!userInput.trim() || !websocket || websocket.readyState !== WebSocket.OPEN) {
      if (!websocket || websocket.readyState !== WebSocket.OPEN) {
        addMessage('error', 'WebSocket이 연결되지 않았습니다. 페이지를 새로고침해주세요.')
      }
      return
    }

    await runFlow()
  }
  
  // 비교 테스트 실행 함수
  const runCompareTest = async () => {
    if (!compareUserInput.trim()) {
      alert('질문을 입력해주세요.')
      return
    }
    
    if (!gptWebsocket || gptWebsocket.readyState !== WebSocket.OPEN ||
        !devstralWebsocket || devstralWebsocket.readyState !== WebSocket.OPEN) {
      alert('WebSocket이 연결되지 않았습니다. 잠시 후 다시 시도해주세요.')
      return
    }
    
    // 상태 초기화
    setCompareRunning(true)
    setGptResult({
      nodeStatuses: {},
      displayedNodeStatuses: {},
      finalResponse: null,
      queryResult: null,
      sqlResult: null,
      sqlQuery: null,
      smq: null,
      error: null
    })
    setDevstralResult({
      nodeStatuses: {},
      displayedNodeStatuses: {},
      finalResponse: null,
      queryResult: null,
      sqlResult: null,
      sqlQuery: null,
      smq: null,
      error: null
    })
    
    // GPT와 Devstral을 동시에 실행
    const gptPromise = runCompareNode('gpt', compareUserInput, gptWebsocket)
    const devstralPromise = runCompareNode('devstral', compareUserInput, devstralWebsocket)
    
    try {
      await Promise.all([gptPromise, devstralPromise])
    } catch (error) {
      console.error('비교 테스트 오류:', error)
    } finally {
      setCompareRunning(false)
    }
  }
  
  // 비교 테스트용 노드 실행 함수
  const runCompareNode = async (provider, inputMessage, ws) => {
    return new Promise((resolve, reject) => {
      const result = {
        nodeStatuses: {},
        displayedNodeStatuses: {},
        finalResponse: null,
        queryResult: null,
        sqlResult: null,
        sqlQuery: null,
        smq: null,
        error: null
      }
      
      const visualQueueRef = []
      const displayedNodeStatuses = {}
      let isCompleted = false // complete 이벤트를 받았는지 추적
      
      const timeout = setTimeout(() => {
        if (!isCompleted) {
          ws.removeEventListener('message', messageHandler)
          result.error = '요청 시간이 초과되었습니다. (5분)'
          if (provider === 'gpt') {
            setGptResult({ ...result, displayedNodeStatuses: { ...displayedNodeStatuses } })
          } else {
            setDevstralResult({ ...result, displayedNodeStatuses: { ...displayedNodeStatuses } })
          }
          reject(new Error('타임아웃'))
        }
      }, 300000)
      
      const messageHandler = (event) => {
        // complete 이벤트를 받은 후에는 더 이상 처리하지 않음
        if (isCompleted) {
          return
        }
        try {
          const data = JSON.parse(event.data)
          const { type, content, step, query_result, sql_result, sql_query, smq, postprocess_result, details } = data
          
          if (type === 'prompt') {
            if (step) {
              result.nodeStatuses[step] = {
                ...result.nodeStatuses[step],
                status: 'running',
                prompt: content
              }
              displayedNodeStatuses[step] = {
                ...displayedNodeStatuses[step],
                status: 'running',
                prompt: content
              }
            }
          } else if (type === 'thought') {
            if (step) {
              const displayContent = (step === 'postprocess' && postprocess_result) ? postprocess_result : content
              result.nodeStatuses[step] = {
                ...result.nodeStatuses[step],
                status: 'complete',
                result: displayContent,
                details: details || null,
                postprocess_result: postprocess_result || null
              }
              displayedNodeStatuses[step] = {
                ...displayedNodeStatuses[step],
                status: 'complete',
                result: displayContent,
                details: details || null,
                postprocess_result: postprocess_result || null
              }
            }
          } else if (type === 'tool_result') {
            if (step) {
              let toolResult
              try {
                toolResult = JSON.parse(content)
              } catch {
                toolResult = content
              }
              result.nodeStatuses[step] = {
                ...result.nodeStatuses[step],
                status: 'complete',
                result: content,
                toolResult: toolResult
              }
              displayedNodeStatuses[step] = {
                ...displayedNodeStatuses[step],
                status: 'complete',
                result: content,
                toolResult: toolResult
              }
            }
          } else if (type === 'error') {
            clearTimeout(timeout)
            ws.removeEventListener('message', messageHandler)
            result.error = content
            if (step) {
              result.nodeStatuses[step] = {
                ...result.nodeStatuses[step],
                status: 'error',
                result: content
              }
              displayedNodeStatuses[step] = {
                ...displayedNodeStatuses[step],
                status: 'error',
                result: content
              }
            }
            if (provider === 'gpt') {
              setGptResult({ ...result, displayedNodeStatuses: { ...displayedNodeStatuses } })
            } else {
              setDevstralResult({ ...result, displayedNodeStatuses: { ...displayedNodeStatuses } })
            }
            reject(new Error(content))
          } else if (type === 'success') {
            // success 이벤트는 중간 단계이므로 상태만 업데이트 (종료하지 않음)
            // success는 주로 respondent 노드에서 발생
            if (step) {
              result.nodeStatuses[step] = {
                ...result.nodeStatuses[step],
                status: 'complete',
                result: content,
                toolResult: {
                  query_result: query_result,
                  sql_result: sql_result,
                  sql_query: sql_query,
                  smq: smq
                }
              }
              displayedNodeStatuses[step] = {
                ...displayedNodeStatuses[step],
                status: 'complete',
                result: content,
                toolResult: {
                  query_result: query_result,
                  sql_result: sql_result,
                  sql_query: sql_query,
                  smq: smq
                }
              }
            }
            // finalResponse는 complete 이벤트에서만 최종적으로 설정
            // success는 중간 단계이므로 임시로만 저장
            if (content) {
              result.finalResponse = content
            }
            if (query_result) result.queryResult = query_result
            if (sql_result) result.sqlResult = sql_result
            if (sql_query) result.sqlQuery = sql_query
            if (smq) result.smq = smq
          } else if (type === 'complete') {
            // complete 이벤트가 오면 종료
            isCompleted = true
            clearTimeout(timeout)
            ws.removeEventListener('message', messageHandler)
            result.finalResponse = content
            result.queryResult = query_result
            result.sqlResult = sql_result
            result.sqlQuery = sql_query
            result.smq = smq
            result.nodeStatuses['complete'] = {
              status: 'complete',
              result: content,
              toolResult: {
                query_result: query_result,
                sql_result: sql_result,
                sql_query: sql_query,
                smq: smq
              }
            }
            displayedNodeStatuses['complete'] = {
              status: 'complete',
              result: content,
              toolResult: {
                query_result: query_result,
                sql_result: sql_result,
                sql_query: sql_query,
                smq: smq
              }
            }
            
            if (provider === 'gpt') {
              setGptResult({ ...result, displayedNodeStatuses: { ...displayedNodeStatuses } })
            } else {
              setDevstralResult({ ...result, displayedNodeStatuses: { ...displayedNodeStatuses } })
            }
            resolve(result)
            return // 핸들러 종료 후 더 이상 처리하지 않음
          }
          
          // 상태 업데이트
          if (provider === 'gpt') {
            setGptResult({ ...result, displayedNodeStatuses: { ...displayedNodeStatuses } })
          } else {
            setDevstralResult({ ...result, displayedNodeStatuses: { ...displayedNodeStatuses } })
          }
        } catch (error) {
          console.error('메시지 파싱 오류:', error)
        }
      }
      
      ws.addEventListener('message', messageHandler)
      
      // 메시지 전송
      ws.send(JSON.stringify({
        message: inputMessage,
        agent_type: 'langgraph',
        prompt_type: '',
        llm_config: provider === 'devstral' ? llmConfig : null
      }))
    })
  }

  return (
    <div className="node-test-page">
      <div className="node-test-header">
        <h2>🔄 노드 테스트</h2>
        <p>여러 프롬프트 노드를 파이프라인으로 순차 실행</p>
        <div className="header-controls">
          <div className="llm-tabs">
            <button
              className={`llm-tab ${llmProvider === 'gpt' ? 'active' : ''}`}
              onClick={() => setLlmProvider('gpt')}
              disabled={loading || running}
            >
              GPT
            </button>
            <button
              className={`llm-tab ${llmProvider === 'devstral' ? 'active' : ''}`}
              onClick={() => setLlmProvider('devstral')}
              disabled={loading || running}
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
      <div className="node-test-tabs">
        <button
          className={`tab-button ${activeTab === 'chat' ? 'active' : ''}`}
          onClick={() => setActiveTab('chat')}
        >
          💬 채팅
        </button>
        <button
          className={`tab-button ${activeTab === 'compare' ? 'active' : ''}`}
          onClick={() => setActiveTab('compare')}
        >
          ⚖️ 비교 테스트
        </button>
        <button
          className={`tab-button ${activeTab === 'prompt' ? 'active' : ''}`}
          onClick={() => setActiveTab('prompt')}
        >
          ⚙️ 프롬프트 관리
        </button>
      </div>

      {activeTab === 'compare' ? (
        <div className="compare-test-container">
          <div className="compare-test-header">
            <h3>GPT vs Devstral 비교 테스트</h3>
            <p>동일한 질문을 GPT와 Devstral에 동시에 전송하여 결과를 비교합니다.</p>
          </div>
          
          <div className="compare-test-input">
            <textarea
              value={compareUserInput}
              onChange={(e) => setCompareUserInput(e.target.value)}
              onKeyPress={(e) => {
                if (e.key === 'Enter' && !e.shiftKey) {
                  e.preventDefault()
                  if (!compareRunning) {
                    runCompareTest()
                  }
                }
              }}
              placeholder="비교할 질문을 입력하세요... (Enter: 전송, Shift+Enter: 줄바꿈)"
              disabled={compareRunning}
            />
            {compareRunning ? (
              <button
                onClick={() => {
                  setCompareRunning(false)
                  if (gptWebsocket) gptWebsocket.close()
                  if (devstralWebsocket) devstralWebsocket.close()
                }}
                className="cancel-button"
              >
                취소
              </button>
            ) : (
              <button
                onClick={runCompareTest}
                disabled={compareRunning || !compareUserInput.trim()}
              >
                {compareRunning ? '처리 중...' : '비교 실행'}
              </button>
            )}
          </div>
          
          <div className="compare-test-results">
            {/* GPT 결과 패널 */}
            <div className="compare-panel compare-panel-gpt">
              <div className="compare-panel-header">
                <h4>🤖 GPT-4o</h4>
                {gptResult?.error && (
                  <span className="compare-error-badge">오류</span>
                )}
                {gptResult?.displayedNodeStatuses?.['complete'] && (
                  <span className="compare-complete-badge">완료</span>
                )}
                {compareRunning && !gptResult?.displayedNodeStatuses?.['complete'] && !gptResult?.error && (
                  <span className="compare-running-badge">실행 중...</span>
                )}
              </div>
              <div className="compare-panel-content">
                {!gptResult && !compareRunning && (
                  <div className="compare-panel-empty">
                    <p>결과가 표시됩니다...</p>
                  </div>
                )}
                {gptResult?.error && (
                  <div className="compare-error-message">
                    <strong>오류:</strong> {gptResult.error}
                  </div>
                )}
                {gptResult?.displayedNodeStatuses && (
                  <CompareResultDisplay 
                    displayedNodeStatuses={gptResult.displayedNodeStatuses}
                    nodeNameMap={nodeNameMap}
                    nodeOrder={nodeOrder}
                  />
                )}
              </div>
            </div>
            
            {/* Devstral 결과 패널 */}
            <div className="compare-panel compare-panel-devstral">
              <div className="compare-panel-header">
                <h4>🦙 Devstral</h4>
                {devstralResult?.error && (
                  <span className="compare-error-badge">오류</span>
                )}
                {devstralResult?.displayedNodeStatuses?.['complete'] && (
                  <span className="compare-complete-badge">완료</span>
                )}
                {compareRunning && !devstralResult?.displayedNodeStatuses?.['complete'] && !devstralResult?.error && (
                  <span className="compare-running-badge">실행 중...</span>
                )}
              </div>
              <div className="compare-panel-content">
                {!devstralResult && !compareRunning && (
                  <div className="compare-panel-empty">
                    <p>결과가 표시됩니다...</p>
                  </div>
                )}
                {devstralResult?.error && (
                  <div className="compare-error-message">
                    <strong>오류:</strong> {devstralResult.error}
                  </div>
                )}
                {devstralResult?.displayedNodeStatuses && (
                  <CompareResultDisplay 
                    displayedNodeStatuses={devstralResult.displayedNodeStatuses}
                    nodeNameMap={nodeNameMap}
                    nodeOrder={nodeOrder}
                  />
                )}
              </div>
            </div>
          </div>
        </div>
      ) : activeTab === 'prompt' ? (
        <div className="prompt-management">
          {/* 프롬프트 타입 선택 UI */}
          <div className="node-selection-area">
            <div className="node-cards">
              {promptTypes.map((promptType) => (
                <div
                  key={promptType.value}
                  className={`node-card ${selectedPromptType === promptType.value ? 'selected' : ''}`}
                  onClick={() => handlePromptTypeChange(promptType.value)}
                >
                  <div className="node-card-header">
                    <span className="node-card-name-display">{promptType.label}</span>
                  </div>
                  <div className="node-card-body">
                    <span className="node-card-prompt-type">{promptType.file}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div className="prompt-editor-container">
            <div className="prompt-editor-header">
              <div className="prompt-type-info">
                <h3>{promptTypes.find(pt => pt.value === selectedPromptType)?.label || '프롬프트'}</h3>
                <span className="prompt-file-name">{promptTypes.find(pt => pt.value === selectedPromptType)?.file || ''}</span>
              </div>
              <button onClick={() => loadPrompt(selectedPromptType)} disabled={promptLoading} className="load-button">
                {promptLoading ? '⏳ 로딩 중...' : '📥 로드'}
              </button>
            </div>
            <textarea
              ref={promptTextareaRef}
              value={promptContent}
              onChange={handleTextareaChange}
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
        <div className="node-test-content">
          <div className="node-test-conversation">
            {Object.keys(nodeStatuses).length === 0 && conversation.length === 0 && (
              <div className="node-test-welcome">
                <p>질문을 입력하면 여러 프롬프트 노드가 파이프라인 형태로 순차 실행됩니다.</p>
              </div>
            )}
            
            {/* 사용자 질문 표시 */}
            {conversation.filter(msg => msg.role === 'user').length > 0 && (
              <div className="user-question-section">
                <div className="user-question-header">
                  <h3>질문</h3>
                  {nodeStatuses['complete'] && nodeStatuses['complete'].status === 'complete' && (
                    <button 
                      className="detail-view-button"
                      onClick={() => setSelectedNodeDetail({ 
                        step: 'complete', 
                        ...nodeStatuses['complete'],
                        allNodes: nodeStatuses
                      })}
                    >
                      상세보기
                    </button>
                  )}
                </div>
                <div className="user-question-content">
                  {conversation.filter(msg => msg.role === 'user').map((msg, idx) => (
                    <div key={idx} className="user-question-text">{msg.content}</div>
                  ))}
                </div>
              </div>
            )}
            
            {/* 에러 메시지 표시 (질문 아래) */}
            {conversation.filter(msg => msg.role === 'error').length > 0 && (
              <div className="error-message-section">
                <div className="error-message-header">
                  <h3>오류 발생</h3>
                  {Object.keys(nodeStatuses).length > 0 && (
                    <button 
                      className="detail-view-button"
                      onClick={() => setSelectedNodeDetail({ 
                        allNodes: nodeStatuses
                      })}
                    >
                      상세보기
                    </button>
                  )}
                </div>
                {conversation.filter(msg => msg.role === 'error').map((msg, idx) => (
                  <div key={idx} className="message message-error">
                    <div className="message-header">
                      <span className="message-role">❌ 오류</span>
                      <span className="message-time">{msg.timestamp}</span>
                    </div>
                    <div className="message-content">
                      {msg.content}
                    </div>
                  </div>
                ))}
              </div>
            )}
            
            {/* 노드 실행 상태 목록 - 모든 노드의 진행 상황 표시 */}
            {(() => {
              // 모든 노드를 실행 순서대로 정렬 (complete 제외)
              const allNodes = Object.keys(displayedNodeStatuses)
                .filter(step => step !== 'complete')
                .sort((a, b) => {
                  const indexA = nodeOrder.indexOf(a)
                  const indexB = nodeOrder.indexOf(b)
                  if (indexA === -1 && indexB === -1) return 0
                  if (indexA === -1) return 1
                  if (indexB === -1) return -1
                  return indexA - indexB
                })
              
              // 실행 중인 노드가 있는지 확인
              const hasRunningNodes = allNodes.some(step => 
                displayedNodeStatuses[step]?.status === 'running'
              )
              
              // 노드가 하나도 없으면 표시하지 않음
              if (allNodes.length === 0) return null
              
              return (
                <div className="node-execution-list">
                  <div className="node-execution-list-header">
                    <h3>실행 진행 상황</h3>
                    {hasRunningNodes && (
                      <span className="execution-status-badge running">실행 중</span>
                    )}
                    {!hasRunningNodes && displayedNodeStatuses['complete'] && (
                      <span className="execution-status-badge complete">완료</span>
                    )}
                  </div>
                  <div className="node-execution-items">
                    {allNodes.map(step => {
                      const nodeStatus = displayedNodeStatuses[step]
                      if (!nodeStatus) return null
                      
                      const nodeName = nodeNameMap[step] || step
                      const status = nodeStatus.status
                      
                      return (
                        <div 
                          key={step}
                          data-node-step={step}
                          className={`node-execution-item node-execution-item-${status}`}
                        >
                          <div className="node-execution-item-icon">
                            {status === 'running' && '🔄'}
                            {status === 'complete' && '✅'}
                            {status === 'error' && '❌'}
                            {!status && '⏸️'}
                          </div>
                          <div className="node-execution-item-info">
                            <div className="node-execution-item-name">{nodeName}</div>
                            <div className="node-execution-item-status">
                              {status === 'running' && '실행 중...'}
                              {status === 'complete' && '완료'}
                              {status === 'error' && '오류 발생'}
                              {!status && '대기 중'}
                            </div>
                          </div>
                        </div>
                      )
                    })}
                  </div>
                </div>
              )
            })()}
            
            {/* 최종 결과 표시 (complete 노드) - 비교 테스트 스타일로 요약 표시 */}
            {displayedNodeStatuses['complete'] && displayedNodeStatuses['complete'].status === 'complete' && (
              <div className="compare-final-result">
                <h5>최종 결과</h5>
                {displayedNodeStatuses['complete'].result && (
                  <div className="compare-result-text">{displayedNodeStatuses['complete'].result}</div>
                )}
                {displayedNodeStatuses['complete'].toolResult && (
                  <div className="compare-result-data">
                    {/* 1. 생성된 예시 데이터 (기본 펼침) */}
                    {displayedNodeStatuses['complete'].toolResult.query_result && (
                      <div className="compare-query-result">
                        <details open>
                          <summary><strong>📊 생성된 예시 데이터</strong></summary>
                          {displayedNodeStatuses['complete'].toolResult.query_result.rows && displayedNodeStatuses['complete'].toolResult.query_result.rows.length > 0 ? (
                            <div className="data-table-container">
                              <table className="data-table">
                                <thead>
                                  <tr>
                                    {displayedNodeStatuses['complete'].toolResult.query_result.columns && displayedNodeStatuses['complete'].toolResult.query_result.columns.map((col, colIdx) => (
                                      <th key={colIdx}>{col}</th>
                                    ))}
                                  </tr>
                                </thead>
                                <tbody>
                                  {displayedNodeStatuses['complete'].toolResult.query_result.rows.map((row, rowIdx) => (
                                    <tr key={rowIdx}>
                                      {displayedNodeStatuses['complete'].toolResult.query_result.columns && displayedNodeStatuses['complete'].toolResult.query_result.columns.map((col, colIdx) => (
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
                    {/* 2. 생성된 SMQ (기본 펼침) */}
                    {displayedNodeStatuses['complete'].toolResult.smq && (
                      <div className="compare-smq">
                        <details open>
                          <summary><strong>📋 생성된 SMQ</strong></summary>
                          <pre className="json-code"><code>{JSON.stringify(displayedNodeStatuses['complete'].toolResult.smq, null, 2)}</code></pre>
                        </details>
                      </div>
                    )}
                    {/* 3. 생성된 SQL 쿼리 */}
                    {displayedNodeStatuses['complete'].toolResult.sql_query && (
                      <div className="compare-sql-query">
                        <details>
                          <summary><strong>🔍 생성된 SQL 쿼리</strong></summary>
                          <pre className="sql-code"><code>{displayedNodeStatuses['complete'].toolResult.sql_query}</code></pre>
                        </details>
                      </div>
                    )}
                    {/* 4. SQL 변환 결과 (메타데이터) */}
                    {displayedNodeStatuses['complete'].toolResult.sql_result && (
                      <div className="compare-sql-result">
                        <details>
                          <summary><strong>🔧 SQL 변환 결과 (메타데이터)</strong></summary>
                          <pre className="json-code"><code>{JSON.stringify(displayedNodeStatuses['complete'].toolResult.sql_result, null, 2)}</code></pre>
                        </details>
                      </div>
                    )}
                  </div>
                )}
              </div>
            )}
            
            <div ref={messagesEndRef} />
          </div>
        </div>
      )}
      
      {/* 팝업 모달 */}
      {selectedNodeDetail && (
        <div className="node-detail-modal-overlay" onClick={() => setSelectedNodeDetail(null)}>
          <div className="node-detail-modal" onClick={(e) => e.stopPropagation()}>
            <div className="node-detail-modal-header">
              <h2>{selectedNodeDetail.allNodes ? '전체 실행 결과' : (nodeNameMap[selectedNodeDetail.step] || selectedNodeDetail.step)}</h2>
              <button className="node-detail-modal-close" onClick={() => setSelectedNodeDetail(null)}>✕</button>
            </div>
            <div className="node-detail-modal-content">
              {/* allNodes가 있으면 모든 노드를 표시 */}
              {selectedNodeDetail.allNodes ? (
                Object.keys(selectedNodeDetail.allNodes)
                  .filter(step => step !== 'complete')
                  .sort((a, b) => {
                    const indexA = nodeOrder.indexOf(a)
                    const indexB = nodeOrder.indexOf(b)
                    if (indexA === -1 && indexB === -1) return 0
                    if (indexA === -1) return 1
                    if (indexB === -1) return -1
                    return indexA - indexB
                  })
                  .map((step) => {
                    const nodeStatus = selectedNodeDetail.allNodes[step]
                    const nodeName = nodeNameMap[step] || step
                    
                    return (
                      <div key={step} className="node-detail-section">
                        <h3>{nodeName}</h3>
                        {nodeStatus.prompt && (
                          <div className="node-detail-subsection">
                            <h4>📝 프롬프트</h4>
                            <pre className="node-detail-prompt">{nodeStatus.prompt}</pre>
                          </div>
                        )}
                        {nodeStatus.result && (
                          <div className="node-detail-subsection">
                            <h4>💬 결과</h4>
                            <div className="node-detail-result">
                              {(() => {
                                // postprocess 노드의 경우 SQL 코드 블록으로 표시
                                if (step === 'postprocess') {
                                  const result = nodeStatus.postprocess_result || nodeStatus.result
                                  const isPass = result && result.toLowerCase().trim() === 'pass'
                                  if (isPass) {
                                    return <div className="text-content"><code>pass</code></div>
                                  }
                                  // SQL인 경우 코드 블록으로 표시
                                  return <pre className="sql-code"><code>{result}</code></pre>
                                }
                                
                                try {
                                  const parsed = JSON.parse(nodeStatus.result)
                                  return <pre className="json-code">{JSON.stringify(parsed, null, 2)}</pre>
                                } catch {
                                  return <div className="text-content">{nodeStatus.result}</div>
                                }
                              })()}
                            </div>
                          </div>
                        )}
                        {/* details 표시 (extractMetrics, extractFilters, extractOrderByAndLimit) */}
                        {nodeStatus.details && (
                          <div className="node-detail-subsection">
                            <h4>📋 추출된 데이터</h4>
                            <div className="node-detail-result">
                              {step === 'extractMetrics' && nodeStatus.details.metrics && (
                                <div className="extracted-data-section">
                                  <h5>📊 메트릭 ({nodeStatus.details.metrics.length}개)</h5>
                                  <ul className="extracted-list">
                                    {nodeStatus.details.metrics.map((metric, idx) => (
                                      <li key={idx} className="extracted-item">
                                        <strong>{metric.name || metric}</strong>
                                        {metric.description && <span className="extracted-desc"> - {metric.description}</span>}
                                      </li>
                                    ))}
                                  </ul>
                                  {nodeStatus.details.group_by && nodeStatus.details.group_by.length > 0 && (
                                    <>
                                      <h5>📐 그룹 바이 ({nodeStatus.details.group_by.length}개)</h5>
                                      <ul className="extracted-list">
                                        {nodeStatus.details.group_by.map((dim, idx) => (
                                          <li key={idx} className="extracted-item">
                                            <strong>{dim.name || dim}</strong>
                                            {dim.description && <span className="extracted-desc"> - {dim.description}</span>}
                                          </li>
                                        ))}
                                      </ul>
                                    </>
                                  )}
                                </div>
                              )}
                              {step === 'extractFilters' && nodeStatus.details.filters && (
                                <div className="extracted-data-section">
                                  <h5>🔍 필터 ({nodeStatus.details.filters.length}개)</h5>
                                  <ul className="extracted-list">
                                    {nodeStatus.details.filters.map((filter, idx) => {
                                      // 문자열인 경우 그대로 표시
                                      if (typeof filter === 'string') {
                                        return (
                                          <li key={idx} className="extracted-item">
                                            <code className="filter-string">{filter}</code>
                                          </li>
                                        )
                                      }
                                      // 객체인 경우 파싱하여 표시
                                      return (
                                        <li key={idx} className="extracted-item">
                                          <strong>{filter.field || filter.column || '필드'}</strong>
                                          {' '}
                                          <span className="filter-operator">{filter.operator || '='}</span>
                                          {' '}
                                          <span className="filter-value">"{filter.value || '값'}"</span>
                                          {filter.description && <span className="extracted-desc"> - {filter.description}</span>}
                                        </li>
                                      )
                                    })}
                                  </ul>
                                </div>
                              )}
                              {step === 'extractOrderByAndLimit' && (
                                <div className="extracted-data-section">
                                  {nodeStatus.details.order_by && nodeStatus.details.order_by.length > 0 && (
                                    <>
                                      <h5>⬆️ 정렬 ({nodeStatus.details.order_by.length}개)</h5>
                                      <ul className="extracted-list">
                                        {nodeStatus.details.order_by.map((order, idx) => {
                                          // 문자열인 경우 파싱하여 표시
                                          if (typeof order === 'string') {
                                            const isDesc = order.startsWith('-')
                                            const field = isDesc ? order.substring(1) : order
                                            const direction = isDesc ? 'DESC' : 'ASC'
                                            return (
                                              <li key={idx} className="extracted-item">
                                                <strong>{field}</strong>
                                                {' '}
                                                <span className="order-direction">{direction}</span>
                                              </li>
                                            )
                                          }
                                          // 객체인 경우 그대로 표시
                                          return (
                                            <li key={idx} className="extracted-item">
                                              <strong>{order.field || order.column || '필드'}</strong>
                                              {' '}
                                              <span className="order-direction">{order.direction || order.order || 'ASC'}</span>
                                            </li>
                                          )
                                        })}
                                      </ul>
                                    </>
                                  )}
                                  {nodeStatus.details.limit !== undefined && nodeStatus.details.limit !== null && (
                                    <>
                                      <h5>🔢 제한</h5>
                                      <div className="extracted-item">
                                        <strong>{nodeStatus.details.limit}</strong>개
                                      </div>
                                    </>
                                  )}
                                </div>
                              )}
                              {/* 기타 details (JSON으로 표시) */}
                              {step !== 'extractMetrics' && step !== 'extractFilters' && step !== 'extractOrderByAndLimit' && (
                                <pre className="json-code">{JSON.stringify(nodeStatus.details, null, 2)}</pre>
                              )}
                            </div>
                          </div>
                        )}
                        {nodeStatus.toolResult && (
                          <div className="node-detail-subsection">
                            <h4>📊 결과 데이터</h4>
                            <pre className="json-code">{JSON.stringify(nodeStatus.toolResult, null, 2)}</pre>
                          </div>
                        )}
                      </div>
                    )
                  })
                  .concat(
                    selectedNodeDetail.allNodes['complete'] ? (
                      <div key="complete" className="node-detail-section">
                        <h3>최종 결과</h3>
                        {selectedNodeDetail.allNodes['complete'].result && (
                          <div className="node-detail-subsection">
                            <h4>💬 결과</h4>
                            <div className="node-detail-result">
                              {(() => {
                                try {
                                  const parsed = JSON.parse(selectedNodeDetail.allNodes['complete'].result)
                                  return <pre className="json-code">{JSON.stringify(parsed, null, 2)}</pre>
                                } catch {
                                  return <div className="text-content">{selectedNodeDetail.allNodes['complete'].result}</div>
                                }
                              })()}
                            </div>
                          </div>
                        )}
                        {selectedNodeDetail.allNodes['complete'].toolResult && (
                          <div className="node-detail-subsection">
                            <h4>📊 결과 데이터</h4>
                            <div className="tool-result-details">
                              {selectedNodeDetail.allNodes['complete'].toolResult.query_result && (
                                <div className="query-result-section">
                                  <details open>
                                    <summary><strong>📊 생성된 예시 데이터</strong></summary>
                                    {selectedNodeDetail.allNodes['complete'].toolResult.query_result.rows && selectedNodeDetail.allNodes['complete'].toolResult.query_result.rows.length > 0 ? (
                                      <div className="data-table-container">
                                        <table className="data-table">
                                          <thead>
                                            <tr>
                                              {selectedNodeDetail.allNodes['complete'].toolResult.query_result.columns && selectedNodeDetail.allNodes['complete'].toolResult.query_result.columns.map((col, colIdx) => (
                                                <th key={colIdx}>{col}</th>
                                              ))}
                                            </tr>
                                          </thead>
                                          <tbody>
                                            {selectedNodeDetail.allNodes['complete'].toolResult.query_result.rows.map((row, rowIdx) => (
                                              <tr key={rowIdx}>
                                                {selectedNodeDetail.allNodes['complete'].toolResult.query_result.columns && selectedNodeDetail.allNodes['complete'].toolResult.query_result.columns.map((col, colIdx) => (
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
                              {selectedNodeDetail.allNodes['complete'].toolResult.sql_query && (
                                <div className="sql-query-section">
                                  <details>
                                    <summary><strong>🔍 생성된 SQL 쿼리</strong></summary>
                                    <pre className="sql-code"><code>{selectedNodeDetail.allNodes['complete'].toolResult.sql_query}</code></pre>
                                  </details>
                                </div>
                              )}
                              {selectedNodeDetail.allNodes['complete'].toolResult.smq && (
                                <div className="smq-section">
                                  <details>
                                    <summary><strong>📋 생성된 SMQ</strong></summary>
                                    <pre className="json-code"><code>{JSON.stringify(selectedNodeDetail.allNodes['complete'].toolResult.smq, null, 2)}</code></pre>
                                  </details>
                                </div>
                              )}
                              {selectedNodeDetail.allNodes['complete'].toolResult.sql_result && (
                                <div className="sql-result-section">
                                  <details>
                                    <summary><strong>🔧 SQL 변환 결과 (메타데이터)</strong></summary>
                                    <pre className="json-code"><code>{JSON.stringify(selectedNodeDetail.allNodes['complete'].toolResult.sql_result, null, 2)}</code></pre>
                                  </details>
                                </div>
                              )}
                            </div>
                          </div>
                        )}
                      </div>
                    ) : null
                  )
              ) : (
                <>
                  {/* 프롬프트 표시 */}
                  {selectedNodeDetail.prompt && (
                    <div className="node-detail-section">
                      <h3>📝 프롬프트</h3>
                      <pre className="node-detail-prompt">{selectedNodeDetail.prompt}</pre>
                    </div>
                  )}
                  
                  {/* 결과 표시 */}
                  {selectedNodeDetail.result && (
                    <div className="node-detail-section">
                      <h3>💬 결과</h3>
                      <div className="node-detail-result">
                        {(() => {
                          // postprocess 노드의 경우 SQL 코드 블록으로 표시
                          if (selectedNodeDetail.step === 'postprocess') {
                            const result = selectedNodeDetail.postprocess_result || selectedNodeDetail.result
                            const isPass = result && result.toLowerCase().trim() === 'pass'
                            if (isPass) {
                              return <div className="text-content"><code>pass</code></div>
                            }
                            // SQL인 경우 코드 블록으로 표시
                            return <pre className="sql-code"><code>{result}</code></pre>
                          }
                          
                          try {
                            const parsed = JSON.parse(selectedNodeDetail.result)
                            return <pre className="json-code">{JSON.stringify(parsed, null, 2)}</pre>
                          } catch {
                            return <div className="text-content">{selectedNodeDetail.result}</div>
                          }
                        })()}
                      </div>
                    </div>
                  )}
                  
                  {/* toolResult 표시 */}
                  {selectedNodeDetail.toolResult && (
                <div className="node-detail-section">
                  <h3>📊 결과 데이터</h3>
                  <div className="tool-result-details">
                    {/* query_result가 있으면 테이블로 표시 */}
                    {selectedNodeDetail.toolResult.query_result && (
                      <div className="query-result-section">
                        <details open>
                          <summary><strong>📊 생성된 예시 데이터</strong></summary>
                          {selectedNodeDetail.toolResult.query_result.rows && selectedNodeDetail.toolResult.query_result.rows.length > 0 ? (
                            <div className="data-table-container">
                              <table className="data-table">
                                <thead>
                                  <tr>
                                    {selectedNodeDetail.toolResult.query_result.columns && selectedNodeDetail.toolResult.query_result.columns.map((col, colIdx) => (
                                      <th key={colIdx}>{col}</th>
                                    ))}
                                  </tr>
                                </thead>
                                <tbody>
                                  {selectedNodeDetail.toolResult.query_result.rows.map((row, rowIdx) => (
                                    <tr key={rowIdx}>
                                      {selectedNodeDetail.toolResult.query_result.columns && selectedNodeDetail.toolResult.query_result.columns.map((col, colIdx) => (
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
                    {selectedNodeDetail.toolResult.sql_query && (
                      <div className="sql-query-section">
                        <details>
                          <summary><strong>🔍 생성된 SQL 쿼리</strong></summary>
                          <pre className="sql-code"><code>{selectedNodeDetail.toolResult.sql_query}</code></pre>
                        </details>
                      </div>
                    )}
                    
                    {/* smq가 있으면 JSON으로 표시 */}
                    {selectedNodeDetail.toolResult.smq && (
                      <div className="smq-section">
                        <details>
                          <summary><strong>📋 생성된 SMQ</strong></summary>
                          <pre className="json-code"><code>{JSON.stringify(selectedNodeDetail.toolResult.smq, null, 2)}</code></pre>
                        </details>
                      </div>
                    )}
                    
                    {/* sql_result가 있으면 메타데이터 표시 */}
                    {selectedNodeDetail.toolResult.sql_result && (
                      <div className="sql-result-section">
                        <details>
                          <summary><strong>🔧 SQL 변환 결과 (메타데이터)</strong></summary>
                          <pre className="json-code"><code>{JSON.stringify(selectedNodeDetail.toolResult.sql_result, null, 2)}</code></pre>
                        </details>
                      </div>
                    )}
                    
                    {/* 기타 toolResult 데이터가 있으면 표시 */}
                    {!selectedNodeDetail.toolResult.query_result && !selectedNodeDetail.toolResult.sql_query && !selectedNodeDetail.toolResult.smq && !selectedNodeDetail.toolResult.sql_result && (
                      <details>
                        <summary>툴 결과</summary>
                        <pre>{JSON.stringify(selectedNodeDetail.toolResult, null, 2)}</pre>
                      </details>
                    )}
                  </div>
                </div>
              )}
                </>
              )}
              
              {/* 상태 표시 */}
              <div className="node-detail-section">
                <h3>상태</h3>
                <div className="node-detail-status">
                  {selectedNodeDetail.status === 'pending' && '⏳ 대기 중'}
                  {selectedNodeDetail.status === 'running' && '🔄 실행 중'}
                  {selectedNodeDetail.status === 'complete' && '✅ 완료'}
                  {selectedNodeDetail.status === 'error' && '❌ 오류'}
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
      
      {activeTab === 'chat' && (
        <div className="node-test-input">
          <textarea
            value={userInput}
            onChange={(e) => setUserInput(e.target.value)}
            onKeyPress={(e) => {
              if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault()
                if (!running) {
                  handleSubmit()
                }
              }
            }}
            placeholder="질문을 입력하세요... (Enter: 전송, Shift+Enter: 줄바꿈)"
            disabled={loading || running}
          />
          {running ? (
            <button
              onClick={cancelFlow}
              className="cancel-button"
            >
              취소
            </button>
          ) : (
            <button
              onClick={handleSubmit}
              disabled={loading || running || !userInput.trim()}
            >
              {loading || running ? '처리 중...' : '전송'}
            </button>
          )}
        </div>
      )}
    </div>
  )
}

export default NodeTest

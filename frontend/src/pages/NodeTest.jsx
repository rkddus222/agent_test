import React, { useState, useRef, useEffect } from 'react'
import axios from 'axios'
import './NodeTest.css'

function NodeTest() {
  const [activeTab, setActiveTab] = useState('chat') // 'chat' or 'prompt'
  const [userInput, setUserInput] = useState('')
  const [conversation, setConversation] = useState([])
  const [loading, setLoading] = useState(false)
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
    { value: 'extract_order_by_and_limit', label: 'Order by & Limit 추출', file: 'extract_order_by_and_limit_prompt.txt' }
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
        setDisplayedNodeStatuses(prev => ({
          ...prev,
          [step]: {
            ...prev[step],
            status: 'running',
            prompt: data.content
          }
        }))
        
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
            
            return {
              ...prev,
              [step]: {
                ...prev[step],
                status: 'complete',
                result: data.content,
                toolResult: data.toolResult || prev[step]?.toolResult,
                details: data.details || prev[step]?.details || null  // details 저장
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
          
          // 모든 running 상태의 노드를 complete로 변경
          Object.keys(updated).forEach(key => {
            if (key !== 'complete' && updated[key]?.status === 'running') {
              updated[key] = {
                ...updated[key],
                status: 'complete'
              }
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
      const timeout = setTimeout(() => {
        websocket.removeEventListener('message', messageHandler)
        reject(new Error('타임아웃'))
      }, 300000)
      
      // 타임아웃을 추적하기 위해 저장
      currentTimeouts.current.push(timeout)

      let nodeResult = null
      let isResolved = false

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
          const { type, content, tool, args, details, step, query_result, sql_result, sql_query, smq } = data
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
              // 백엔드 실제 상태 업데이트 (details도 함께 저장)
              setNodeStatuses(prev => ({
                ...prev,
                [step]: {
                  ...prev[step],
                  status: 'complete',
                  result: content,
                  details: details || null  // details 저장 (metrics, filters, order_by 등)
                }
              }))
              
              // Visual Queue에 추가 (details 포함)
              visualQueueRef.current.push({
                step,
                eventType: 'thought',
                data: { content, details: details || null },
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
        prompt_type: '' // LangGraph 에이전트는 내부적으로 프롬프트를 관리하므로 prompt_type 불필요
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
      if (!cancelled) {
        addMessage('error', `❌ Flow 실행 오류: ${error.message}`)
      }
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

  return (
    <div className="node-test-page">
      <div className="node-test-header">
        <h2>🔄 노드 테스트</h2>
        <p>여러 프롬프트 노드를 파이프라인으로 순차 실행</p>
        <div className="ws-status">
          <span className={wsConnected ? 'status-connected' : 'status-disconnected'}>
            {wsConnected ? '🟢 연결됨' : '🔴 연결 안 됨'}
          </span>
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
          className={`tab-button ${activeTab === 'prompt' ? 'active' : ''}`}
          onClick={() => setActiveTab('prompt')}
        >
          ⚙️ 프롬프트 관리
        </button>
      </div>

      {activeTab === 'prompt' ? (
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
            
            {/* 노드 실행 카드 - 현재 실행 중인 노드만 표시 (Visual Queue 처리된 상태) */}
            {(() => {
              // displayedNodeStatuses에서 현재 실행 중인 노드 찾기
              const runningNodes = Object.keys(displayedNodeStatuses)
                .filter(step => {
                  const status = displayedNodeStatuses[step]?.status
                  return step !== 'complete' && status === 'running'
                })
                .sort((a, b) => {
                  const indexA = nodeOrder.indexOf(a)
                  const indexB = nodeOrder.indexOf(b)
                  if (indexA === -1 && indexB === -1) return 0
                  if (indexA === -1) return 1
                  if (indexB === -1) return -1
                  return indexA - indexB
                })
              
              // 실행 중인 노드 중 가장 앞선 노드 (실행 순서상 첫 번째)
              const currentRunningNode = runningNodes.length > 0 ? runningNodes[0] : null
              
              if (!currentRunningNode) return null
              
              const nodeStatus = displayedNodeStatuses[currentRunningNode]
              const nodeName = nodeNameMap[currentRunningNode] || currentRunningNode
              
              return (
                <div className="node-running-card-container">
                  <div key={currentRunningNode} className="node-running-card">
                    <div className="node-running-card-header">
                      <div className="node-running-card-icon">🔄</div>
                      <div className="node-running-card-title">{nodeName}</div>
                    </div>
                    <div className="node-running-card-body">
                      <div className="node-running-card-status">실행 중...</div>
                    </div>
                  </div>
                </div>
              )
            })()}
            
            {/* 최종 결과 표시 (complete 노드) - displayedNodeStatuses 사용 */}
            {displayedNodeStatuses['complete'] && displayedNodeStatuses['complete'].status === 'complete' && (
              <div className="final-result-section">
                <h3>최종 결과</h3>
                <div className="final-result-content">
                  {displayedNodeStatuses['complete'].result && (
                    <div className="final-result-text">{displayedNodeStatuses['complete'].result}</div>
                  )}
                  {displayedNodeStatuses['complete'].toolResult && (
                    <div className="final-result-data">
                      {/* query_result가 있으면 테이블로 표시 */}
                      {displayedNodeStatuses['complete'].toolResult.query_result && (
                        <div className="query-result-section">
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
                      
                      {/* sql_query가 있으면 코드 블록으로 표시 */}
                      {displayedNodeStatuses['complete'].toolResult.sql_query && (
                        <div className="sql-query-section">
                          <details>
                            <summary><strong>🔍 생성된 SQL 쿼리</strong></summary>
                            <pre className="sql-code"><code>{displayedNodeStatuses['complete'].toolResult.sql_query}</code></pre>
                          </details>
                        </div>
                      )}
                      
                      {/* smq가 있으면 JSON으로 표시 */}
                      {displayedNodeStatuses['complete'].toolResult.smq && (
                        <div className="smq-section">
                          <details>
                            <summary><strong>📋 생성된 SMQ</strong></summary>
                            <pre className="json-code"><code>{JSON.stringify(displayedNodeStatuses['complete'].toolResult.smq, null, 2)}</code></pre>
                          </details>
                        </div>
                      )}
                      
                      {/* sql_result가 있으면 메타데이터 표시 */}
                      {displayedNodeStatuses['complete'].toolResult.sql_result && (
                        <div className="sql-result-section">
                          <details>
                            <summary><strong>🔧 SQL 변환 결과 (메타데이터)</strong></summary>
                            <pre className="json-code"><code>{JSON.stringify(displayedNodeStatuses['complete'].toolResult.sql_result, null, 2)}</code></pre>
                          </details>
                        </div>
                      )}
                    </div>
                  )}
                </div>
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
                                    {nodeStatus.details.filters.map((filter, idx) => (
                                      <li key={idx} className="extracted-item">
                                        <strong>{filter.field || filter.column || '필드'}</strong>
                                        {' '}
                                        <span className="filter-operator">{filter.operator || '='}</span>
                                        {' '}
                                        <span className="filter-value">"{filter.value || '값'}"</span>
                                        {filter.description && <span className="extracted-desc"> - {filter.description}</span>}
                                      </li>
                                    ))}
                                  </ul>
                                </div>
                              )}
                              {step === 'extractOrderByAndLimit' && (
                                <div className="extracted-data-section">
                                  {nodeStatus.details.order_by && nodeStatus.details.order_by.length > 0 && (
                                    <>
                                      <h5>⬆️ 정렬 ({nodeStatus.details.order_by.length}개)</h5>
                                      <ul className="extracted-list">
                                        {nodeStatus.details.order_by.map((order, idx) => (
                                          <li key={idx} className="extracted-item">
                                            <strong>{order.field || order.column || '필드'}</strong>
                                            {' '}
                                            <span className="order-direction">{order.direction || order.order || 'ASC'}</span>
                                          </li>
                                        ))}
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

import React, { useState, useRef, useEffect } from 'react'
import ReactMarkdown from 'react-markdown'
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter'
import { vscDarkPlus } from 'react-syntax-highlighter/dist/cjs/styles/prism'
import './ChatInterface.css'

function ChatInterface({ promptType = "test", onConnectionChange, llmConfig = null }) {
  const [messages, setMessages] = useState([])
  const [input, setInput] = useState('')
  const [isConnected, setIsConnected] = useState(false)
  const [currentSteps, setCurrentSteps] = useState([])
  const [isProcessing, setIsProcessing] = useState(false)
  const [streamingContent, setStreamingContent] = useState('')
  const wsRef = useRef(null)
  const messagesEndRef = useRef(null)
  const skipDeltaRef = useRef(false) // success/message 후 delta 스킵 플래그

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }

  const connectWebSocket = () => {
    // 이미 연결되어 있으면 다시 연결하지 않음
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      return
    }

    // 개발 환경에서는 직접 백엔드 서버(포트 8000)에 연결
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const host = window.location.hostname
    // 개발 환경에서는 항상 포트 8000으로 직접 연결
    const wsUrl = `${protocol}//${host}:8000/ws/chat`
    
    console.log('WebSocket 연결 시도:', wsUrl)
    const ws = new WebSocket(wsUrl)

    ws.onopen = () => {
      console.log('WebSocket 연결됨')
      setIsConnected(true)
      if (onConnectionChange) {
        onConnectionChange(true)
      }
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
      setIsConnected(false)
      if (onConnectionChange) {
        onConnectionChange(false)
      }
    }

    ws.onclose = (event) => {
      console.log('WebSocket 연결 종료:', event.code, event.reason)
      setIsConnected(false)
      if (onConnectionChange) {
        onConnectionChange(false)
      }
      
      // 비정상 종료인 경우 에러 메시지 표시는 하지 않음 (자동 재연결 없음)
    }

    wsRef.current = ws
  }

  // 컴포넌트 마운트 시 WebSocket 자동 연결
  useEffect(() => {
    connectWebSocket()

    return () => {
      // 컴포넌트 언마운트 시 연결 종료
      if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
        wsRef.current.close()
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // 연결 상태 변경 시 부모 컴포넌트에 알림
  useEffect(() => {
    if (onConnectionChange) {
      onConnectionChange(isConnected)
    }
  }, [isConnected, onConnectionChange])

  useEffect(() => {
    scrollToBottom()
  }, [messages, currentSteps, streamingContent])

  const handleCancel = () => {
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify({ type: 'cancel' }))
      setIsProcessing(false)
      setStreamingContent('')
      setCurrentSteps([])
      skipDeltaRef.current = false
    }
  }

  const handleWebSocketMessage = (data) => {
    const { type, content, tool, args, steps, query_result, sql_result, sql_query, smq } = data

    if (type === 'delta') {
      // skipDeltaRef가 true면 delta를 무시 (success/message가 이미 content를 추가했음)
      if (skipDeltaRef.current) {
        return
      }
      setStreamingContent(prev => prev + (content || ''))
      return
    }

    if (type === 'cancelled') {
      setMessages(prev => [...prev, {
        role: 'assistant',
        content: content || '작업이 취소되었습니다.',
        steps: currentSteps
      }])
      setCurrentSteps([])
      setIsProcessing(false)
      setStreamingContent('')
      skipDeltaRef.current = false
      return
    }

    if (type === 'complete') {
      setMessages(prev => [...prev, {
        role: 'assistant',
        content: content || streamingContent,
        steps: steps || currentSteps,
        query_result: query_result,  // executeQuery에서 생성한 예시 데이터
        sql_result: sql_result,  // SQL 변환 결과
        sql_query: sql_query,  // 생성된 SQL 쿼리
        smq: smq  // 생성된 SMQ
      }])
      setCurrentSteps([])
      setIsProcessing(false)
      setStreamingContent('')
      skipDeltaRef.current = false
    } else if (type === 'thought') {
      setCurrentSteps(prev => [...prev, { type: 'thought', content }])
    } else if (type === 'tool_call') {
      setCurrentSteps(prev => [...prev, {
        type: 'tool_call',
        content,
        tool,
        args
      }])
    } else if (type === 'tool_result') {
      setCurrentSteps(prev => [...prev, { type: 'tool_result', content }])
    } else if (type === 'error') {
      setMessages(prev => [...prev, {
        role: 'assistant',
        content: `Error: ${content}`,
        steps: currentSteps
      }])
      setCurrentSteps([])
      setIsProcessing(false)
      setStreamingContent('')
      skipDeltaRef.current = false
    } else if (type === 'success') {
      // success 이벤트의 content를 즉시 streamingContent에 추가
      if (content) {
        setStreamingContent(prev => {
          const newContent = prev ? prev + '\n\n' + content : content
          return newContent
        })
        // 이후 delta 이벤트는 무시 (이미 content를 받았으므로)
        skipDeltaRef.current = true
      }
      setCurrentSteps(prev => [...prev, { type: 'success', content }])
    } else if (type === 'message') {
      // message 이벤트의 content를 즉시 streamingContent에 추가
      if (content) {
        setStreamingContent(prev => {
          const newContent = prev ? prev + '\n\n' + content : content
          return newContent
        })
        // 이후 delta 이벤트는 무시 (이미 content를 받았으므로)
        skipDeltaRef.current = true
      }
      setCurrentSteps(prev => [...prev, { type: 'message', content }])
    }
  }

  const handleSend = () => {
    if (!input.trim() || isProcessing) return

    setIsProcessing(true)

    if (!isConnected || !wsRef.current) {
      connectWebSocket()
      setTimeout(() => {
        sendMessage()
      }, 500)
    } else {
      sendMessage()
    }
  }

  const sendMessage = () => {
    const userMessage = input.trim()
    setInput('')
    setMessages(prev => [...prev, { role: 'user', content: userMessage }])
    setCurrentSteps([])
    setStreamingContent('')
    skipDeltaRef.current = false // 새 메시지 시작 시 초기화

    const messageData = {
      message: userMessage,
      prompt_type: promptType
    }
    if (llmConfig) {
      messageData.llm_config = llmConfig
    }
    
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify(messageData))
    } else {
      connectWebSocket()
      setTimeout(() => {
        if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
          wsRef.current.send(JSON.stringify(messageData))
        }
      }, 500)
    }
  }

  const handleKeyPress = (e) => {
    if (e.key === 'Enter' && !e.shiftKey && !isProcessing) {
      e.preventDefault()
      handleSend()
    }
  }

  return (
    <div className="chat-interface">
      <div className="chat-messages">
        {messages.map((message, index) => (
          <div key={index} className={`message message-${message.role}`}>
            <div className="message-header">
              <strong>{message.role === 'user' ? 'You' : 'Assistant'}</strong>
            </div>
            {message.steps && message.steps.length > 0 && (
              <div className="message-steps">
                {message.steps.map((step, stepIndex) => (
                  <div key={stepIndex} className="step">
                    {step.type === 'thought' && (
                      <details className="step-details">
                        <summary>Reasoning</summary>
                        <div className="step-content">{step.content}</div>
                      </details>
                    )}
                    {step.type === 'tool_call' && (
                      <details className="step-details" open>
                        <summary>Executing Tool: {step.tool}</summary>
                        {step.args && (
                          <pre className="step-json">
                            {JSON.stringify(step.args, null, 2)}
                          </pre>
                        )}
                      </details>
                    )}
                    {step.type === 'tool_result' && (
                      <details className="step-details">
                        <summary>Tool Result</summary>
                        <div className="step-content">
                          {(() => {
                            try {
                              const resultData = JSON.parse(step.content)
                              if (resultData.diff) {
                                return (
                                  <SyntaxHighlighter
                                    language="diff"
                                    style={vscDarkPlus}
                                  >
                                    {resultData.diff}
                                  </SyntaxHighlighter>
                                )
                              }
                            } catch (e) {
                              // Not JSON or no diff
                            }
                            return (
                              <SyntaxHighlighter
                                language="json"
                                style={vscDarkPlus}
                              >
                                {step.content}
                              </SyntaxHighlighter>
                            )
                          })()}
                        </div>
                      </details>
                    )}
                    {step.type === 'success' && (
                      <div className="step-success">
                        <strong>✅ Success:</strong>
                        <ReactMarkdown
                          components={{
                            code: ({ node, inline, className, children, ...props }) => {
                              const match = /language-(\w+)/.exec(className || '')
                              return !inline && match ? (
                                <SyntaxHighlighter
                                  style={vscDarkPlus}
                                  language={match[1]}
                                  PreTag="div"
                                  {...props}
                                >
                                  {String(children).replace(/\n$/, '')}
                                </SyntaxHighlighter>
                              ) : (
                                <code className={className} {...props}>
                                  {children}
                                </code>
                              )
                            }
                          }}
                        >
                          {step.content}
                        </ReactMarkdown>
                      </div>
                    )}
                  </div>
                ))}
              </div>
            )}
            <div className="message-content">
              <ReactMarkdown
                components={{
                  code: ({ node, inline, className, children, ...props }) => {
                    const match = /language-(\w+)/.exec(className || '')
                    return !inline && match ? (
                      <SyntaxHighlighter
                        style={vscDarkPlus}
                        language={match[1]}
                        PreTag="div"
                        {...props}
                      >
                        {String(children).replace(/\n$/, '')}
                      </SyntaxHighlighter>
                    ) : (
                      <code className={className} {...props}>
                        {children}
                      </code>
                    )
                  }
                }}
              >
                {message.content}
              </ReactMarkdown>
            </div>
          </div>
        ))}
        {currentSteps.length > 0 && (
          <div className="message message-assistant">
            <div className="message-header">
              <strong>Assistant</strong> <span className="typing-indicator">(처리 중...)</span>
            </div>
            {streamingContent && (
              <div className="message-content">
                <ReactMarkdown
                  components={{
                    code: ({ node, inline, className, children, ...props }) => {
                      const match = /language-(\w+)/.exec(className || '')
                      return !inline && match ? (
                        <SyntaxHighlighter
                          style={vscDarkPlus}
                          language={match[1]}
                          PreTag="div"
                          {...props}
                        >
                          {String(children).replace(/\n$/, '')}
                        </SyntaxHighlighter>
                      ) : (
                        <code className={className} {...props}>
                          {children}
                        </code>
                      )
                    }
                  }}
                >
                  {streamingContent}
                </ReactMarkdown>
              </div>
            )}
            <div className="message-steps">
              {currentSteps.map((step, stepIndex) => (
                <div key={stepIndex} className="step">
                  {step.type === 'thought' && (
                    <details className="step-details" open>
                      <summary>Reasoning</summary>
                      <div className="step-content">{step.content}</div>
                    </details>
                  )}
                  {step.type === 'tool_call' && (
                    <details className="step-details" open>
                      <summary>Executing Tool: {step.tool}...</summary>
                      {step.args && (
                        <pre className="step-json">
                          {JSON.stringify(step.args, null, 2)}
                        </pre>
                      )}
                    </details>
                  )}
                  {step.type === 'tool_result' && (
                    <details className="step-details" open>
                      <summary>Tool Result</summary>
                      <div className="step-content">
                        {(() => {
                          try {
                            const resultData = JSON.parse(step.content)
                            if (resultData.diff) {
                              return (
                                <SyntaxHighlighter
                                  language="diff"
                                  style={vscDarkPlus}
                                >
                                  {resultData.diff}
                                </SyntaxHighlighter>
                              )
                            }
                          } catch (e) {
                            // Not JSON or no diff
                          }
                          return (
                            <SyntaxHighlighter
                              language="json"
                              style={vscDarkPlus}
                            >
                              {step.content}
                            </SyntaxHighlighter>
                          )
                        })()}
                      </div>
                    </details>
                  )}
                  {step.type === 'success' && (
                    <div className="step-success">
                      <strong>✅ Success:</strong>
                      <ReactMarkdown
                        components={{
                          code: ({ node, inline, className, children, ...props }) => {
                            const match = /language-(\w+)/.exec(className || '')
                            return !inline && match ? (
                              <SyntaxHighlighter
                                style={vscDarkPlus}
                                language={match[1]}
                                PreTag="div"
                                {...props}
                              >
                                {String(children).replace(/\n$/, '')}
                              </SyntaxHighlighter>
                            ) : (
                              <code className={className} {...props}>
                                {children}
                              </code>
                            )
                          }
                        }}
                      >
                        {step.content}
                      </ReactMarkdown>
                    </div>
                  )}
                </div>
              ))}
            </div>
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>
      <div className="chat-input-container">
        <textarea
          className={`chat-input ${isProcessing ? 'chat-input-disabled' : ''}`}
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyPress={handleKeyPress}
          placeholder={isProcessing ? "응답을 기다리는 중입니다..." : "모델에서 변경하고 싶은 내용을 입력하세요"}
          rows={3}
          disabled={isProcessing}
        />
        <div className="chat-buttons">
          {isProcessing ? (
            <button
              className="cancel-button"
              onClick={handleCancel}
            >
              취소
            </button>
          ) : (
            <button
              className={`chat-send-button ${isProcessing ? 'chat-send-button-processing' : ''}`}
              onClick={handleSend}
              disabled={!input.trim()}
            >
              전송
            </button>
          )}
        </div>
      </div>
    </div>
  )
}

export default ChatInterface


import React, { useState, useEffect, useRef } from 'react'
import ChatInterface from '../components/ChatInterface'
import PromptEditor from '../components/PromptEditor'
import './Test.css'

function Test() {
  const [activeTab, setActiveTab] = useState('chat')
  const [wsConnected, setWsConnected] = useState(false)
  const wsRef = useRef(null)
  
  // LLM 설정 상태
  const [llmProvider, setLlmProvider] = useState('devstral') // 'gpt' or 'devstral'
  const [llmConfig, setLlmConfig] = useState({
    url: 'http://183.102.124.135:8001/',
    model_name: '/home/daquv/.cache/huggingface/hub/models--unsloth--Devstral-Small-2507-unsloth-bnb-4bit/snapshots/0578b9b52309df8ae455eb860a6cebe50dc891cd',
    model_type: 'vllm',
    temperature: 0.1,
    max_tokens: 1000
  })

  // WebSocket 연결 상태 확인
  useEffect(() => {
    const checkWebSocketConnection = () => {
      // ChatInterface의 WebSocket 연결 상태를 확인하기 위해
      // 주기적으로 체크하거나, ChatInterface에서 상태를 전달받아야 함
      // 간단하게 ChatInterface 내부의 wsRef를 확인하는 방법은 없으므로
      // ChatInterface 컴포넌트를 수정하여 연결 상태를 콜백으로 전달받거나
      // 또는 Test 페이지에서 직접 WebSocket을 연결하여 상태를 관리
      
      // 일단 ChatInterface가 마운트되면 연결을 시도하도록 함
      // ChatInterface 내부에서 연결 상태를 관리하므로, 
      // ChatInterface 컴포넌트에 연결 상태를 props로 전달받을 수 있도록 수정 필요
    }

    // ChatInterface가 활성화되어 있을 때만 체크
    if (activeTab === 'chat') {
      checkWebSocketConnection()
    }
  }, [activeTab])

  return (
    <div className="test-page">
      <div className="test-header">
        <h2>🧪 테스트</h2>
        <p>시멘틱 에이전트 테스트</p>
        {activeTab === 'chat' && (
          <div className="header-controls">
            <div className="llm-tabs">
              <button
                className={`llm-tab ${llmProvider === 'gpt' ? 'active' : ''}`}
                onClick={() => setLlmProvider('gpt')}
              >
                GPT
              </button>
              <button
                className={`llm-tab ${llmProvider === 'devstral' ? 'active' : ''}`}
                onClick={() => setLlmProvider('devstral')}
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
        )}
      </div>

      {/* 탭 메뉴 */}
      <div className="test-tabs">
        <button
          className={`test-tab ${activeTab === 'chat' ? 'active' : ''}`}
          onClick={() => setActiveTab('chat')}
        >
          💬 채팅
        </button>
        <button
          className={`test-tab ${activeTab === 'prompt' ? 'active' : ''}`}
          onClick={() => setActiveTab('prompt')}
        >
          ⚙️ 프롬프트 설정
        </button>
      </div>

      <div className="test-content">
        {activeTab === 'chat' && (
          <ChatInterface 
            onConnectionChange={(connected) => setWsConnected(connected)}
            llmConfig={llmProvider === 'devstral' ? llmConfig : null}
          />
        )}
        {activeTab === 'prompt' && <PromptEditor />}
      </div>
    </div>
  )
}

export default Test


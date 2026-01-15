import React, { useState } from 'react'
import ChatInterface from './components/ChatInterface'
import Sidebar from './components/Sidebar'
import PromptEditor from './components/PromptEditor'
import YMLManagement from './pages/YMLManagement'
import Test from './pages/Test'
import AgentTest from './pages/AgentTest'
import SMQTest from './pages/SMQTest'
import NodeTest from './pages/NodeTest'
import Evaluation from './pages/Evaluation'
import PostProcessTest from './pages/PostProcessTest'
import './App.css'

function App() {
  const [activeTab, setActiveTab] = useState('chat')
  const [activePage, setActivePage] = useState('yml-management')

  const renderPageContent = () => {
    if (activePage === 'yml-management') {
      return <YMLManagement />
    } else if (activePage === 'test') {
      return <Test />
    } else if (activePage === 'agent-test') {
      return <AgentTest />
    } else if (activePage === 'smq-test') {
      return <SMQTest />
    } else if (activePage === 'node-test') {
      return <NodeTest />
    } else if (activePage === 'evaluation') {
      return <Evaluation />
    } else if (activePage === 'postprocess-test') {
      return <PostProcessTest />
    } else {
      return (
        <>
          <div className="tabs">
            <button
              className={activeTab === 'chat' ? 'active' : ''}
              onClick={() => setActiveTab('chat')}
            >
              Chat Interface
            </button>
            <button
              className={activeTab === 'prompt' ? 'active' : ''}
              onClick={() => setActiveTab('prompt')}
            >
              Prompt Settings
            </button>
          </div>
          <div className="tab-content">
            {activeTab === 'chat' && <ChatInterface />}
            {activeTab === 'prompt' && <PromptEditor />}
          </div>
        </>
      )
    }
  }

  return (
    <div className="app">
      <header className="app-header">
        <h1>Semantic Model Engineer Agent</h1>
      </header>
      <div className="app-container">
        <aside className="sidebar">
          <Sidebar activePage={activePage} onPageChange={setActivePage} />
        </aside>
        <main className="main-content">
          {renderPageContent()}
        </main>
      </div>
    </div>
  )
}

export default App



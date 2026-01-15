import React from 'react'
import './Sidebar.css'

function Sidebar({ activePage, onPageChange }) {
  return (
    <div className="sidebar-menu">
      <div className="sidebar-header">
        <h2>메뉴</h2>
      </div>
      
      <nav className="sidebar-nav">
        <div className="sidebar-section">
          <h3 className="sidebar-section-title">YML 파일 관리</h3>
          <ul className="sidebar-menu-list">
            <li>
              <button
                className={`sidebar-menu-item ${activePage === 'yml-management' ? 'active' : ''}`}
                onClick={() => onPageChange('yml-management')}
              >
                📝 YML 파일 관리
              </button>
            </li>
          </ul>
        </div>

        <div className="sidebar-section">
          <h3 className="sidebar-section-title">테스트</h3>
          <ul className="sidebar-menu-list">
            <li>
              <button
                className={`sidebar-menu-item ${activePage === 'test' ? 'active' : ''}`}
                onClick={() => onPageChange('test')}
              >
                🧪 테스트
              </button>
            </li>
            <li>
              <button
                className={`sidebar-menu-item ${activePage === 'agent-test' ? 'active' : ''}`}
                onClick={() => onPageChange('agent-test')}
              >
                🤖 에이전트 테스트
              </button>
            </li>
            <li>
              <button
                className={`sidebar-menu-item ${activePage === 'smq-test' ? 'active' : ''}`}
                onClick={() => onPageChange('smq-test')}
              >
                🔍 SMQ 테스트
              </button>
            </li>
            <li>
              <button
                className={`sidebar-menu-item ${activePage === 'node-test' ? 'active' : ''}`}
                onClick={() => onPageChange('node-test')}
              >
                🔄 노드 테스트
              </button>
            </li>
            <li>
              <button
                className={`sidebar-menu-item ${activePage === 'postprocess-test' ? 'active' : ''}`}
                onClick={() => onPageChange('postprocess-test')}
              >
                📊 후처리 테스트
              </button>
            </li>
          </ul>
        </div>

        <div className="sidebar-section">
          <h3 className="sidebar-section-title">평가</h3>
          <ul className="sidebar-menu-list">
            <li>
              <button
                className={`sidebar-menu-item ${activePage === 'evaluation' ? 'active' : ''}`}
                onClick={() => onPageChange('evaluation')}
              >
                📊 평가
              </button>
            </li>
          </ul>
        </div>
      </nav>
    </div>
  )
}

export default Sidebar


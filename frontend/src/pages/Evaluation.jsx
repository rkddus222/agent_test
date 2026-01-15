import React, { useState, useEffect, useRef } from 'react'
import axios from 'axios'
import ReactMarkdown from 'react-markdown'
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter'
import { vscDarkPlus } from 'react-syntax-highlighter/dist/esm/styles/prism'
import './Evaluation.css'

function Evaluation() {
  const [uploadedFiles, setUploadedFiles] = useState([])
  const [uploadMode, setUploadMode] = useState('files') // 'files' or 'folder'
  const [isEvaluating, setIsEvaluating] = useState(false)
  const [evaluationResult, setEvaluationResult] = useState(null)
  const [evaluationCriteria, setEvaluationCriteria] = useState('')
  const [evaluationLogs, setEvaluationLogs] = useState([])
  const [currentStep, setCurrentStep] = useState('')
  const wsRef = useRef(null)

  const handleFileUpload = (event) => {
    const files = Array.from(event.target.files)
    const fileData = files.map(file => ({
      name: file.name,
      path: file.webkitRelativePath || file.name, // 폴더 구조 경로 유지
      size: file.size,
      type: file.type,
      file: file
    }))
    setUploadedFiles(prev => [...prev, ...fileData])
  }

  const handleFolderUpload = (event) => {
    const files = Array.from(event.target.files)
    if (files.length === 0) return
    
    const fileData = files
      .filter(file => file.size > 0) // 디렉토리 항목 제외
      .map(file => ({
        name: file.name,
        path: file.webkitRelativePath || file.name, // 폴더 구조 경로 유지
        size: file.size,
        type: file.type,
        file: file
      }))
    setUploadedFiles(prev => [...prev, ...fileData])
  }

  const handleDrop = (e) => {
    e.preventDefault()
    const items = Array.from(e.dataTransfer.items)
    
    items.forEach(item => {
      if (item.kind === 'file') {
        const entry = item.webkitGetAsEntry()
        if (entry) {
          processEntry(entry)
        }
      }
    })
  }

  const processEntry = (entry, path = '') => {
    if (entry.isFile) {
      entry.file(file => {
        const filePath = path ? `${path}/${file.name}` : file.name
        setUploadedFiles(prev => [...prev, {
          name: file.name,
          path: filePath,
          size: file.size,
          type: file.type,
          file: file
        }])
      })
    } else if (entry.isDirectory) {
      const reader = entry.createReader()
      reader.readEntries(entries => {
        entries.forEach(subEntry => {
          const newPath = path ? `${path}/${entry.name}` : entry.name
          processEntry(subEntry, newPath)
        })
      })
    }
  }

  const handleDragOver = (e) => {
    e.preventDefault()
  }

  const handleRemoveFile = (index) => {
    setUploadedFiles(prev => prev.filter((_, i) => i !== index))
  }

  const handleStartEvaluation = async () => {
    if (uploadedFiles.length === 0) {
      alert('평가할 파일을 업로드해주세요.')
      return
    }

    setIsEvaluating(true)
    setEvaluationResult(null)
    setEvaluationLogs([])
    setCurrentStep('파일 업로드 중...')

    try {
      // 1단계: 파일 업로드
      const formData = new FormData()
      uploadedFiles.forEach((fileData) => {
        formData.append('files', fileData.file)
      })
      
      const pathsJson = JSON.stringify(uploadedFiles.map((f, idx) => ({ 
        index: idx, 
        name: f.name, 
        path: f.path 
      })))
      formData.append('paths_json', pathsJson)

      const uploadResponse = await axios.post('/api/evaluation/upload', formData, {
        headers: {
          'Content-Type': 'multipart/form-data'
        }
      })

      if (!uploadResponse.data.success) {
        throw new Error('파일 업로드 실패')
      }

      const { temp_dir, file_paths } = uploadResponse.data
      setCurrentStep('평가 시작...')

      // 2단계: WebSocket으로 평가 진행
      const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
      const wsUrl = `${wsProtocol}//${window.location.host}/ws/evaluation`
      const ws = new WebSocket(wsUrl)
      wsRef.current = ws

      ws.onopen = () => {
        setCurrentStep('평가 진행 중...')
        addLog('info', '평가를 시작합니다.')
        
        // WebSocket으로 평가 시작 신호 전송
        ws.send(JSON.stringify({
          temp_dir: temp_dir,
          criteria: evaluationCriteria.trim() || undefined,
          file_paths: file_paths
        }))
      }

      ws.onmessage = (event) => {
        const data = JSON.parse(event.data)
        const type = data.type
        const content = data.content || ''

        switch (type) {
          case 'thought':
            addLog('reasoning', content)
            // Step 정보 추출
            if (content.includes('Step 1')) {
              setCurrentStep('Step 1: 규칙 파악 중...')
            } else if (content.includes('Step 2')) {
              setCurrentStep('Step 2: 기준 데이터 확인 중...')
            } else if (content.includes('Step 3')) {
              setCurrentStep('Step 3: 결과 대조 및 채점 중...')
            }
            break
          
          case 'tool_call':
            const tool = data.tool || ''
            const args = data.args || {}
            if (tool === 'readFile') {
              addLog('action', `📖 파일 읽기: ${args.path || '알 수 없음'}`)
            } else if (tool === 'submitReport') {
              addLog('action', '📊 평가 리포트 제출 중...')
            }
            break
          
          case 'tool_result':
            try {
              const result = JSON.parse(content)
              if (result.error) {
                addLog('error', `❌ 오류: ${result.error}`)
              } else if (result.content) {
                addLog('success', `✅ 파일 읽기 완료 (${result.content.substring(0, 50)}...)`)
              }
            } catch (e) {
              addLog('info', `📄 ${content.substring(0, 100)}...`)
            }
            break
          
          case 'success':
            try {
              const reportData = JSON.parse(content)
              addLog('success', `✅ 평가 완료! 점수: ${reportData.score || 'N/A'}점`)
            } catch (e) {
              addLog('success', '✅ 평가가 완료되었습니다!')
            }
            break
          
          case 'complete':
            setEvaluationResult({
              success: true,
              result: content
            })
            setCurrentStep('평가 완료')
            addLog('success', '🎉 평가가 성공적으로 완료되었습니다!')
            ws.close()
            break
          
          case 'error':
            setEvaluationResult({
              success: false,
              error: content
            })
            addLog('error', `❌ 오류: ${content}`)
            setCurrentStep('오류 발생')
            ws.close()
            break
        }
      }

      ws.onerror = (error) => {
        console.error('WebSocket 오류:', error)
        addLog('error', 'WebSocket 연결 오류가 발생했습니다.')
        setCurrentStep('연결 오류')
        setIsEvaluating(false)
      }

      ws.onclose = () => {
        setIsEvaluating(false)
        wsRef.current = null
      }

    } catch (error) {
      console.error('평가 실패:', error)
      addLog('error', `❌ 평가 실패: ${error.response?.data?.detail || error.message}`)
      setEvaluationResult({
        success: false,
        error: error.response?.data?.detail || error.message
      })
      setIsEvaluating(false)
      setCurrentStep('실패')
      
      if (wsRef.current) {
        wsRef.current.close()
        wsRef.current = null
      }
    }
  }

  const addLog = (type, message) => {
    const timestamp = new Date().toLocaleTimeString()
    setEvaluationLogs(prev => [...prev, { type, message, timestamp }])
  }

  useEffect(() => {
    // 컴포넌트 언마운트 시 WebSocket 정리
    return () => {
      if (wsRef.current) {
        wsRef.current.close()
      }
    }
  }, [])

  const formatFileSize = (bytes) => {
    if (bytes === 0) return '0 Bytes'
    const k = 1024
    const sizes = ['Bytes', 'KB', 'MB', 'GB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i]
  }

  return (
    <div className="evaluation">
      <div className="evaluation-header">
        <div className="evaluation-header-content">
          <h1>📊 평가 시스템</h1>
          <p className="evaluation-subtitle">지원자가 제출한 파일들을 LLM을 통해 평가합니다</p>
        </div>
      </div>

      <div className="evaluation-content">
        <div className="evaluation-section">
          <div className="evaluation-section-header">
            <h2>📁 파일/폴더 등록</h2>
            <p className="section-description">평가할 파일이나 폴더를 업로드해주세요</p>
          </div>

          <div className="upload-mode-selector">
            <button
              className={`upload-mode-button ${uploadMode === 'files' ? 'active' : ''}`}
              onClick={() => {
                setUploadMode('files')
                setUploadedFiles([])
              }}
              disabled={isEvaluating}
            >
              📄 파일 선택
            </button>
            <button
              className={`upload-mode-button ${uploadMode === 'folder' ? 'active' : ''}`}
              onClick={() => {
                setUploadMode('folder')
                setUploadedFiles([])
              }}
              disabled={isEvaluating}
            >
              📁 폴더 선택
            </button>
          </div>

          <div 
            className="file-upload-area"
            onDrop={handleDrop}
            onDragOver={handleDragOver}
          >
            {uploadMode === 'files' ? (
              <>
                <input
                  type="file"
                  id="file-upload"
                  multiple
                  onChange={handleFileUpload}
                  className="file-upload-input"
                  disabled={isEvaluating}
                />
                <label htmlFor="file-upload" className="file-upload-label">
                  <div className="file-upload-icon">📎</div>
                  <div className="file-upload-text">
                    <strong>클릭하여 파일 선택</strong>
                    <span>또는 드래그 앤 드롭</span>
                  </div>
                </label>
              </>
            ) : (
              <>
                <input
                  type="file"
                  id="folder-upload"
                  webkitdirectory=""
                  directory=""
                  multiple
                  onChange={handleFolderUpload}
                  className="file-upload-input"
                  disabled={isEvaluating}
                />
                <label htmlFor="folder-upload" className="file-upload-label">
                  <div className="file-upload-icon">📁</div>
                  <div className="file-upload-text">
                    <strong>클릭하여 폴더 선택</strong>
                    <span>폴더 전체 구조가 유지됩니다</span>
                  </div>
                </label>
              </>
            )}
          </div>

          {uploadedFiles.length > 0 && (
            <div className="uploaded-files-list">
              <h3>업로드된 파일 목록 ({uploadedFiles.length}개)</h3>
              <div className="files-list">
                {uploadedFiles.map((fileData, index) => {
                  const isInFolder = fileData.path.includes('/')
                  const pathParts = fileData.path.split('/')
                  const displayName = pathParts[pathParts.length - 1]
                  const folderPath = pathParts.slice(0, -1).join('/')
                  
                  return (
                    <div key={index} className="file-item">
                      <div className="file-item-info">
                        <span className="file-item-icon">
                          {isInFolder ? '📂' : '📄'}
                        </span>
                        <div className="file-item-details">
                          <span className="file-item-name" title={fileData.path}>
                            {isInFolder && (
                              <span className="file-item-folder">{folderPath}/</span>
                            )}
                            {displayName}
                          </span>
                          <span className="file-item-size">{formatFileSize(fileData.size)}</span>
                        </div>
                      </div>
                      <button
                        className="file-item-remove"
                        onClick={() => handleRemoveFile(index)}
                        disabled={isEvaluating}
                      >
                        ✕
                      </button>
                    </div>
                  )
                })}
              </div>
            </div>
          )}
        </div>

        <div className="evaluation-section">
          <div className="evaluation-section-header">
            <h2>📋 평가 기준 (선택사항)</h2>
            <p className="section-description">평가에 사용할 기준을 입력해주세요</p>
          </div>

          <textarea
            className="evaluation-criteria-input"
            placeholder="예: 코드 품질, 알고리즘 이해도, 문제 해결 능력 등을 평가해주세요."
            value={evaluationCriteria}
            onChange={(e) => setEvaluationCriteria(e.target.value)}
            rows={6}
            disabled={isEvaluating}
          />
        </div>

        <div className="evaluation-section">
          <button
            className="evaluation-start-button"
            onClick={handleStartEvaluation}
            disabled={isEvaluating || uploadedFiles.length === 0}
          >
            {isEvaluating ? (
              <>
                <span className="spinner"></span>
                {currentStep || '평가 중...'}
              </>
            ) : (
              <>
                🚀 평가 시작
              </>
            )}
          </button>
        </div>

        {/* 평가 진행 로그 */}
        {(isEvaluating || evaluationLogs.length > 0) && (
          <div className="evaluation-section">
            <div className="evaluation-section-header">
              <h2>📋 평가 진행 로그</h2>
              <p className="section-description">평가 과정을 실시간으로 확인할 수 있습니다</p>
            </div>
            
            <div className="evaluation-logs">
              <div className="logs-header">
                <span className="current-step">{currentStep}</span>
                {evaluationLogs.length > 0 && (
                  <button
                    className="clear-logs-button"
                    onClick={() => setEvaluationLogs([])}
                    disabled={isEvaluating}
                  >
                    로그 지우기
                  </button>
                )}
              </div>
              <div className="logs-content">
                {evaluationLogs.length === 0 ? (
                  <div className="log-empty">아직 로그가 없습니다...</div>
                ) : (
                  evaluationLogs.map((log, index) => (
                    <div key={index} className={`log-item log-${log.type}`}>
                      <span className="log-timestamp">{log.timestamp}</span>
                      <span className="log-message">{log.message}</span>
                    </div>
                  ))
                )}
              </div>
            </div>
          </div>
        )}

        {evaluationResult && (
          <div className="evaluation-section">
            <div className="evaluation-section-header">
              <h2>📊 평가 결과</h2>
            </div>

            <div className={`evaluation-result ${evaluationResult.success ? 'success' : 'error'}`}>
              {evaluationResult.success ? (
                <div className="evaluation-result-content">
                  {evaluationResult.result && typeof evaluationResult.result === 'string' ? (
                    <div className="result-markdown">
                      <ReactMarkdown
                        components={{
                          code({ node, inline, className, children, ...props }) {
                            const match = /language-(\w+)/.exec(className || '')
                            const codeString = String(children).replace(/\n$/, '')
                            
                            if (!inline && match && SyntaxHighlighter) {
                              return (
                                <SyntaxHighlighter
                                  style={vscDarkPlus}
                                  language={match[1]}
                                  PreTag="div"
                                  {...props}
                                >
                                  {codeString}
                                </SyntaxHighlighter>
                              )
                            }
                            
                            return (
                              <code className={className} {...props}>
                                {children}
                              </code>
                            )
                          },
                          h1: ({ node, ...props }) => <h1 className="result-h1" {...props} />,
                          h2: ({ node, ...props }) => <h2 className="result-h2" {...props} />,
                          h3: ({ node, ...props }) => <h3 className="result-h3" {...props} />,
                          p: ({ node, ...props }) => <p className="result-p" {...props} />,
                          ul: ({ node, ...props }) => <ul className="result-ul" {...props} />,
                          ol: ({ node, ...props }) => <ol className="result-ol" {...props} />,
                          li: ({ node, ...props }) => <li className="result-li" {...props} />,
                          strong: ({ node, ...props }) => <strong className="result-strong" {...props} />,
                          em: ({ node, ...props }) => <em className="result-em" {...props} />,
                          blockquote: ({ node, ...props }) => <blockquote className="result-blockquote" {...props} />,
                          hr: ({ node, ...props }) => <hr className="result-hr" {...props} />,
                        }}
                      >
                        {evaluationResult.result}
                      </ReactMarkdown>
                    </div>
                  ) : (
                    <div className="result-json">
                      <pre>{JSON.stringify(evaluationResult.result || evaluationResult, null, 2)}</pre>
                    </div>
                  )}
                </div>
              ) : (
                <div className="evaluation-result-error">
                  <p>❌ 평가 중 오류가 발생했습니다:</p>
                  <p>{evaluationResult.error || '알 수 없는 오류'}</p>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

export default Evaluation

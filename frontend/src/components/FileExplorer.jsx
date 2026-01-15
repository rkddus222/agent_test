import React, { useState, useEffect } from 'react'
import axios from 'axios'
import './FileExplorer.css'

function FileExplorer({ selectedFile, onSelectFile }) {
  const [files, setFiles] = useState([])
  const [fileContent, setFileContent] = useState('')
  const [editMode, setEditMode] = useState(false)
  const [editedContent, setEditedContent] = useState('')
  const [newFileName, setNewFileName] = useState('')
  const [showDDLModal, setShowDDLModal] = useState(false)
  const [ddlDialect, setDdlDialect] = useState('bigquery')
  const [ddlText, setDdlText] = useState('')
  const [parseLoading, setParseLoading] = useState(false)
  const [ddlLoading, setDdlLoading] = useState(false)

  useEffect(() => {
    loadFiles()
  }, [])

  useEffect(() => {
    if (selectedFile) {
      loadFileContent(selectedFile)
    }
  }, [selectedFile])

  const loadFiles = async () => {
    try {
      const response = await axios.get('/api/files')
      setFiles(response.data.files)
    } catch (error) {
      console.error('파일 목록 로드 실패:', error)
    }
  }

  const loadFileContent = async (filePath) => {
    try {
      const response = await axios.get(`/api/files/${filePath}`)
      setFileContent(response.data.content)
      setEditedContent(response.data.content)
    } catch (error) {
      console.error('파일 내용 로드 실패:', error)
    }
  }

  const handleFileSelect = (filePath) => {
    onSelectFile(filePath)
    setEditMode(false)
  }

  const handleSave = async () => {
    try {
      await axios.post('/api/files/save', {
        path: selectedFile,
        content: editedContent
      })
      setFileContent(editedContent)
      setEditMode(false)
      alert('파일이 저장되었습니다!')
    } catch (error) {
      alert('파일 저장 실패: ' + error.response?.data?.detail || error.message)
    }
  }

  const handleDelete = async () => {
    if (!confirm('정말 이 파일을 삭제하시겠습니까?')) return

    try {
      await axios.post('/api/files/delete', { file_path: selectedFile })
      setFiles(files.filter(f => f !== selectedFile))
      onSelectFile(null)
      setFileContent('')
    } catch (error) {
      alert('파일 삭제 실패: ' + error.response?.data?.detail || error.message)
    }
  }

  const handleRename = async () => {
    if (!newFileName) {
      alert('새 파일 이름을 입력해주세요.')
      return
    }

    try {
      await axios.post('/api/files/rename', {
        old_path: selectedFile,
        new_filename: newFileName
      })
      await loadFiles()
      onSelectFile(newFileName.endsWith('.yml') ? newFileName : newFileName + '.yml')
      setNewFileName('')
      alert('파일 이름이 변경되었습니다!')
    } catch (error) {
      alert('파일 이름 변경 실패: ' + error.response?.data?.detail || error.message)
    }
  }

  const handleCreateFromDDL = async () => {
    if (!ddlText.trim()) {
      alert('DDL 문을 입력해주세요.')
      return
    }
    
    setDdlLoading(true)
    try {
      await axios.post('/api/ddl/create', {
        dialect: ddlDialect,
        ddl_text: ddlText
      })
      await loadFiles()
      setDdlText('')
      setShowDDLModal(false)
      alert('DDL에서 파일이 생성되었습니다!')
    } catch (error) {
      alert('DDL 생성 실패: ' + (error.response?.data?.detail || error.message))
    } finally {
      setDdlLoading(false)
    }
  }

  const handleParse = async () => {
    setParseLoading(true)
    try {
      const response = await axios.post('/api/parse')
      if (response.data.success) {
        alert(`✅ 파싱 완료!\n📊 ${response.data.semantic_models_count}개 모델, ${response.data.metrics_count}개 메트릭`)
      } else {
        alert('❌ 파싱 실패: ' + response.data.error)
      }
    } catch (error) {
      alert('파싱 실패: ' + error.response?.data?.detail || error.message)
    } finally {
      setParseLoading(false)
    }
  }

  return (
    <div className="file-explorer">
      <div className="file-explorer-header">
        <h2>Playground Files</h2>
      </div>

      <div className="file-explorer-section">
        <button
          className="parse-button"
          onClick={handleParse}
          disabled={parseLoading}
        >
          {parseLoading ? '파싱 중...' : '📦 Manifest 생성'}
        </button>
      </div>

      <div className="file-explorer-section">
        <button
          className="create-file-toggle"
          onClick={() => setShowDDLModal(true)}
        >
          ➕ 새 파일 만들기
        </button>
      </div>

      {/* DDL 모달 */}
      {showDDLModal && (
        <div className="modal-overlay" onClick={() => setShowDDLModal(false)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h3>DDL에서 Semantic Model 생성</h3>
              <button
                className="modal-close"
                onClick={() => setShowDDLModal(false)}
              >
                ×
              </button>
            </div>
            <div className="modal-body">
              <div className="ddl-form">
                <label>
                  DBMS 타입
                  <select
                    value={ddlDialect}
                    onChange={(e) => setDdlDialect(e.target.value)}
                  >
                    <option value="bigquery">BigQuery</option>
                    <option value="mysql">MySQL</option>
                    <option value="postgresql">PostgreSQL</option>
                    <option value="oracle">Oracle</option>
                    <option value="mssql">MSSQL</option>
                  </select>
                </label>
                <label>
                  DDL 문 입력
                  <textarea
                    placeholder="-- bigquery&#10;CREATE TABLE `project.dataset.table_name` (&#10;  `id` INTEGER NOT NULL,&#10;  `name` STRING,&#10;  `amount` DECIMAL(10, 2),&#10;  PRIMARY KEY (`id`)&#10;);"
                    value={ddlText}
                    onChange={(e) => setDdlText(e.target.value)}
                    rows={15}
                  />
                </label>
              </div>
            </div>
            <div className="modal-footer">
              <button
                className="modal-button-cancel"
                onClick={() => {
                  setShowDDLModal(false)
                  setDdlText('')
                }}
              >
                취소
              </button>
              <button
                className="modal-button-submit"
                onClick={handleCreateFromDDL}
                disabled={ddlLoading || !ddlText.trim()}
              >
                {ddlLoading ? '생성 중...' : 'DDL에서 생성'}
              </button>
            </div>
          </div>
        </div>
      )}

      <div className="file-explorer-section">
        <select
          className="file-selector"
          value={selectedFile || ''}
          onChange={(e) => handleFileSelect(e.target.value)}
        >
          <option value="">파일 선택</option>
          {files.map((file) => (
            <option key={file} value={file}>
              {file}
            </option>
          ))}
        </select>
      </div>

      {selectedFile && (
        <div className="file-content-section">
          <div className="file-actions">
            <label>
              <input
                type="checkbox"
                checked={editMode}
                onChange={(e) => setEditMode(e.target.checked)}
              />
              편집 모드
            </label>
            <button className="delete-button" onClick={handleDelete}>
              🚨 영구 삭제
            </button>
          </div>

          {editMode && (
            <div className="rename-section">
              <input
                type="text"
                placeholder="새 파일 이름"
                value={newFileName}
                onChange={(e) => setNewFileName(e.target.value)}
              />
              <button onClick={handleRename}>📝 이름 변경</button>
            </div>
          )}

          {editMode ? (
            <div className="file-editor">
              <textarea
                value={editedContent}
                onChange={(e) => setEditedContent(e.target.value)}
                className="file-editor-textarea"
              />
              <div className="editor-actions">
                <button onClick={handleSave}>💾 저장</button>
                <button onClick={() => {
                  setEditedContent(fileContent)
                  setEditMode(false)
                }}>
                  🔄 되돌리기
                </button>
              </div>
            </div>
          ) : (
            <pre className="file-content">{fileContent}</pre>
          )}
        </div>
      )}
    </div>
  )
}

export default FileExplorer


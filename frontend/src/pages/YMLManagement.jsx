import React, { useState, useEffect } from 'react'
import axios from 'axios'
import ChatInterface from '../components/ChatInterface'
import EditableTable from '../components/EditableTable'
import { parseYML, stringifyYML, extractTableData, convertTableDataToYML } from '../utils/ymlParser'
import './YMLManagement.css'

function YMLManagement() {
  const [files, setFiles] = useState([])
  const [selectedFile, setSelectedFile] = useState(null)
  const [fileContent, setFileContent] = useState('')
  const [editMode, setEditMode] = useState(false)
  const [editedContent, setEditedContent] = useState('')
  const [newFileName, setNewFileName] = useState('')
  const [showDDLModal, setShowDDLModal] = useState(false)
  const [showChatPanel, setShowChatPanel] = useState(false)
  const [ddlDialect, setDdlDialect] = useState('postgresql')
  const [ddlText, setDdlText] = useState('')
  const [parseLoading, setParseLoading] = useState(false)
  const [ddlLoading, setDdlLoading] = useState(false)
  const [modalTab, setModalTab] = useState('ddl') // 'ddl' or 'yml'
  const [ymlContent, setYmlContent] = useState('')
  const [modalFileName, setModalFileName] = useState('')
  const [ymlLoading, setYmlLoading] = useState(false)
  const [loadingFiles, setLoadingFiles] = useState(true)
  const [viewMode, setViewMode] = useState('table') // 'table' or 'text'
  const [tableData, setTableData] = useState(null)
  const [parsedYmlData, setParsedYmlData] = useState(null)
  const [rowEditModal, setRowEditModal] = useState(null) // { section, data, index, columns, modelIndex }

  useEffect(() => {
    loadFiles()
  }, [])

  useEffect(() => {
    if (selectedFile) {
      loadFileContent(selectedFile)
      setEditMode(true)
    }
  }, [selectedFile])

  const loadFiles = async () => {
    setLoadingFiles(true)
    try {
      const response = await axios.get('/api/files')
      setFiles(response.data.files)
    } catch (error) {
      console.error('파일 목록 로드 실패:', error)
      alert('파일 목록을 불러오는데 실패했습니다.')
    } finally {
      setLoadingFiles(false)
    }
  }

  const loadFileContent = async (filePath) => {
    try {
      const response = await axios.get(`/api/files/${filePath}`)
      const content = response.data.content
      setFileContent(content)
      setEditedContent(content)
      
      // YML 파싱 시도
      try {
        const parsed = parseYML(content)
        setParsedYmlData(parsed)
        const extracted = extractTableData(parsed)
        setTableData(extracted)
      } catch (parseError) {
        console.warn('YML 파싱 실패 (텍스트 모드로 표시):', parseError)
        setParsedYmlData(null)
        setTableData(null)
      }
    } catch (error) {
      // 404 에러인 경우 (파일이 아직 존재하지 않음)는 조용히 처리
      // 파일 이름 변경 직후에는 파일 목록이 업데이트되었지만
      // 파일 시스템에 완전히 반영되기 전일 수 있음
      if (error.response?.status === 404) {
        console.log('파일을 찾을 수 없습니다. 잠시 후 다시 시도하세요:', filePath)
        // 잠시 후 재시도
        setTimeout(async () => {
          try {
            const retryResponse = await axios.get(`/api/files/${filePath}`)
            const content = retryResponse.data.content
            setFileContent(content)
            setEditedContent(content)
            
            try {
              const parsed = parseYML(content)
              setParsedYmlData(parsed)
              const extracted = extractTableData(parsed)
              setTableData(extracted)
            } catch (parseError) {
              console.warn('YML 파싱 실패 (텍스트 모드로 표시):', parseError)
              setParsedYmlData(null)
              setTableData(null)
            }
          } catch (retryError) {
            console.error('파일 내용 로드 재시도 실패:', retryError)
            alert('파일 내용을 불러오는데 실패했습니다.')
          }
        }, 500)
      } else {
        console.error('파일 내용 로드 실패:', error)
        alert('파일 내용을 불러오는데 실패했습니다.')
      }
    }
  }

  const handleFileSelect = (filePath) => {
    setSelectedFile(filePath)
    setEditMode(true)
    setNewFileName('')
  }

  const handleSave = async () => {
    try {
      let contentToSave = editedContent
      
      // 표 형식 모드인 경우 YML로 변환
      if (viewMode === 'table' && tableData && parsedYmlData) {
        try {
          const updatedYmlData = convertTableDataToYML(tableData, parsedYmlData)
          contentToSave = stringifyYML(updatedYmlData)
        } catch (convertError) {
          console.error('YML 변환 실패:', convertError)
          alert('❌ 표 형식 데이터를 YML로 변환하는데 실패했습니다. 텍스트 모드로 전환하여 저장해주세요.')
          return
        }
      }
      
      await axios.post('/api/files/save', {
        path: selectedFile,
        content: contentToSave
      })
      setFileContent(contentToSave)
      setEditedContent(contentToSave)
      
      // 저장 후 다시 파싱
      try {
        const parsed = parseYML(contentToSave)
        setParsedYmlData(parsed)
        const extracted = extractTableData(parsed)
        setTableData(extracted)
      } catch (parseError) {
        console.warn('저장 후 파싱 실패:', parseError)
      }
      
      alert('✅ 파일이 저장되었습니다!')
      await loadFiles()
    } catch (error) {
      alert('❌ 파일 저장 실패: ' + (error.response?.data?.detail || error.message))
    }
  }
  
  const handleTableDataChange = (section, newData, modelIndex = 0) => {
    if (!tableData) return
    
    const updated = { ...tableData }
    if (section === 'entities' || section === 'dimensions' || section === 'measures') {
      if (!updated.semanticModels[modelIndex]) return
      if (section === 'entities') {
        updated.semanticModels[modelIndex].entities = newData
      } else if (section === 'dimensions') {
        updated.semanticModels[modelIndex].dimensions = newData
      } else if (section === 'measures') {
        updated.semanticModels[modelIndex].measures = newData
      }
    } else if (section === 'metrics') {
      updated.metrics = newData
    }
    setTableData(updated)
  }
  
  const handleAddRow = (section, newRow, modelIndex = 0, columns) => {
    setRowEditModal({
      section,
      data: { ...newRow },
      index: -1, // -1 indicates new row
      columns,
      modelIndex
    })
  }

  const handleDelete = async () => {
    if (!confirm(`정말 "${selectedFile}" 파일을 삭제하시겠습니까?\n이 작업은 되돌릴 수 없습니다.`)) return

    try {
      await axios.post('/api/files/delete', { file_path: selectedFile })
      setFiles(files.filter(f => f !== selectedFile))
      setSelectedFile(null)
      setFileContent('')
      alert('✅ 파일이 삭제되었습니다!')
    } catch (error) {
      alert('❌ 파일 삭제 실패: ' + (error.response?.data?.detail || error.message))
    }
  }

  const handleRename = async () => {
    if (!newFileName.trim()) {
      alert('새 파일 이름을 입력해주세요.')
      return
    }

    try {
      await axios.post('/api/files/rename', {
        old_path: selectedFile,
        new_filename: newFileName.trim()
      })
      
      // 파일 목록 먼저 업데이트
      await loadFiles()
      
      // 기존 경로의 디렉토리 부분 유지하고 파일명만 변경
      const oldPathParts = selectedFile.split('/')
      const oldDir = oldPathParts.slice(0, -1).join('/')
      const newFileNameWithExt = newFileName.trim().endsWith('.yml') 
        ? newFileName.trim() 
        : newFileName.trim() + '.yml'
      const newPath = oldDir ? `${oldDir}/${newFileNameWithExt}` : newFileNameWithExt
      
      setNewFileName('')
      
      // 새로운 파일 경로로 변경 (useEffect가 자동으로 loadFileContent 호출)
      // 파일 목록이 업데이트되었으므로 약간의 지연 후 setSelectedFile을 호출하여
      // useEffect의 loadFileContent가 성공하도록 함
      setTimeout(() => {
        setSelectedFile(newPath)
      }, 200)
      
      alert('✅ 파일 이름이 변경되었습니다!')
    } catch (error) {
      alert('❌ 파일 이름 변경 실패: ' + (error.response?.data?.detail || error.message))
    }
  }

  const handleCreateFromDDL = async () => {
    if (!ddlText.trim()) {
      alert('DDL 문을 입력해주세요.')
      return
    }
    
    if (!modalFileName.trim()) {
      alert('파일 이름을 입력해주세요.')
      return
    }
    
    setDdlLoading(true)
    try {
      const response = await axios.post('/api/ddl/create', {
        dialect: ddlDialect,
        ddl_text: ddlText,
        filename: modalFileName.trim()
      })
      await loadFiles()
      setDdlText('')
      setModalFileName('')
      setShowDDLModal(false)
      alert(`✅ ${response.data.message || 'DDL에서 파일이 생성되었습니다!'}`)
    } catch (error) {
      alert('❌ DDL 생성 실패: ' + (error.response?.data?.detail || error.message))
    } finally {
      setDdlLoading(false)
    }
  }

  const handleCreateFromYML = async () => {
    if (!ymlContent.trim()) {
      alert('YML 내용을 입력해주세요.')
      return
    }
    
    if (!modalFileName.trim()) {
      alert('파일 이름을 입력해주세요.')
      return
    }
    
    setYmlLoading(true)
    try {
      const response = await axios.post('/api/files', {
        filename: modalFileName.trim(),
        content: ymlContent.trim()
      })
      await loadFiles()
      setYmlContent('')
      setModalFileName('')
      setShowDDLModal(false)
      alert(`✅ ${response.data.message || 'YML 파일이 생성되었습니다!'}`)
    } catch (error) {
      alert('❌ YML 파일 생성 실패: ' + (error.response?.data?.detail || error.message))
    } finally {
      setYmlLoading(false)
    }
  }

  const handleParse = async () => {
    setParseLoading(true)
    try {
      const response = await axios.post('/api/parse')
      if (response.data.success) {
        alert(`✅ 파싱 완료!\n📊 ${response.data.semantic_models_count}개 모델, ${response.data.metrics_count}개 메트릭`)
        await loadFiles()
      } else {
        alert('❌ 파싱 실패: ' + response.data.error)
      }
    } catch (error) {
      alert('❌ 파싱 실패: ' + (error.response?.data?.detail || error.message))
    } finally {
      setParseLoading(false)
    }
  }

  const handleRowClick = (section, row, index, columns, modelIndex = 0) => {
    setRowEditModal({
      section,
      data: { ...row },
      index,
      columns,
      modelIndex
    })
  }

  const handleRowSave = () => {
    if (!rowEditModal) return
    
    const { section, data, index, modelIndex } = rowEditModal
    const isNew = index === -1
    
    if (section === 'metrics') {
      const newMetrics = tableData.metrics ? [...tableData.metrics] : []
      if (isNew) {
        newMetrics.push({
          ...data,
          id: `metric-${Date.now()}-${Math.random()}`
        })
      } else {
        newMetrics[index] = data
      }
      handleTableDataChange('metrics', newMetrics)
    } else {
      const newModels = [...tableData.semanticModels]
      const model = newModels[modelIndex]
      
      if (section === 'entities') {
        if (!model.entities) model.entities = []
        if (isNew) {
          model.entities.push({
            ...data,
            id: `entity-${Date.now()}-${Math.random()}`
          })
        } else {
          model.entities[index] = data
        }
      } else if (section === 'dimensions') {
        if (!model.dimensions) model.dimensions = []
        if (isNew) {
          model.dimensions.push({
            ...data,
            id: `dim-${Date.now()}-${Math.random()}`
          })
        } else {
          model.dimensions[index] = data
        }
      } else if (section === 'measures') {
        if (!model.measures) model.measures = []
        if (isNew) {
          model.measures.push({
            ...data,
            id: `measure-${Date.now()}-${Math.random()}`
          })
        } else {
          model.measures[index] = data
        }
      }
      
      setTableData({ ...tableData, semanticModels: newModels })
    }
    setRowEditModal(null)
  }

  const handleRowModalChange = (key, value) => {
    if (!rowEditModal) return
    setRowEditModal({
      ...rowEditModal,
      data: {
        ...rowEditModal.data,
        [key]: value
      }
    })
  }

  return (
    <div className="yml-management">
      <div className="yml-header">
        <div className="yml-header-content">
          <h1>YML 파일 관리</h1>
          <p className="yml-subtitle">Semantic Model 파일을 생성, 편집 및 관리합니다</p>
        </div>
        <div className="yml-header-actions">
          <button
            className="action-button action-button-primary"
            onClick={handleParse}
            disabled={parseLoading}
          >
            {parseLoading ? '⏳ 파싱 중...' : '📦 Manifest 생성'}
          </button>
          <button
            className="action-button action-button-secondary"
            onClick={() => setShowChatPanel(true)}
          >
            🤖 자동 생성
          </button>
        </div>
      </div>

      <div className="yml-content">
        <div className="yml-sidebar">
          <div className="yml-sidebar-section">
            <div className="yml-sidebar-header">
              <h3>파일 목록</h3>
              <button
                className="icon-button"
                onClick={loadFiles}
                title="새로고침"
              >
                🔄
              </button>
            </div>
            
            <div className="yml-action-buttons">
              <button
                className="create-button"
                onClick={() => setShowDDLModal(true)}
              >
                ➕ 새 파일
              </button>
            </div>

            <div className="file-list-container">
              {loadingFiles ? (
                <div className="loading-state">파일 목록을 불러오는 중...</div>
              ) : files.length === 0 ? (
                <div className="empty-state">
                  <p>📁 파일이 없습니다</p>
                  <p className="empty-state-subtitle">새 파일을 생성해보세요</p>
                </div>
              ) : (
                <div className="file-list">
                  {files.map((file) => (
                    <div
                      key={file}
                      className={`file-item ${selectedFile === file ? 'selected' : ''}`}
                      onClick={() => handleFileSelect(file)}
                    >
                      <span className="file-icon">📄</span>
                      <span className="file-name">{file}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>

        <div className="yml-editor">
          {selectedFile ? (
            <>
              <div className="yml-editor-header">
                <div className="editor-header-left">
                  <h3 className="editor-title">{selectedFile}</h3>
                </div>
                <div className="editor-header-actions">
                  <div className="view-mode-toggle">
                    <button
                      className={`view-mode-button ${viewMode === 'table' ? 'active' : ''}`}
                      onClick={() => setViewMode('table')}
                      disabled={!tableData}
                      title={!tableData ? 'YML 파싱에 실패하여 표 형식으로 볼 수 없습니다' : '표 형식 보기'}
                    >
                      📊 표 형식
                    </button>
                    <button
                      className={`view-mode-button ${viewMode === 'text' ? 'active' : ''}`}
                      onClick={() => setViewMode('text')}
                      title="텍스트 형식 보기"
                    >
                      📝 텍스트
                    </button>
                  </div>
                  <div className="rename-controls">
                    <input
                      type="text"
                      className="rename-input"
                      placeholder="새 파일 이름"
                      value={newFileName}
                      onChange={(e) => setNewFileName(e.target.value)}
                      onKeyPress={(e) => e.key === 'Enter' && handleRename()}
                    />
                    <button
                      className="rename-button"
                      onClick={handleRename}
                    >
                      📝 이름 변경
                    </button>
                  </div>
                  <button
                    className="delete-button"
                    onClick={handleDelete}
                  >
                    🗑️ 삭제
                  </button>
                </div>
              </div>

              <div className="yml-editor-content">
                {viewMode === 'table' && tableData && (tableData.semanticModels.length > 0 || (tableData.metrics && tableData.metrics.length > 0)) ? (
                  <div className="table-view-wrapper">
                    <div className="table-view-content">
                      {tableData.semanticModels.map((model, modelIndex) => (
                        <div key={modelIndex} className="semantic-model-section">
                          <div className="model-info-section">
                            <h4>Semantic Model 정보</h4>
                            <div className="model-info-grid">
                              <div className="model-info-item">
                                <label>Name:</label>
                                <input
                                  type="text"
                                  value={model.name}
                                  onChange={(e) => {
                                    const updated = { ...tableData }
                                    updated.semanticModels[modelIndex].name = e.target.value
                                    setTableData(updated)
                                  }}
                                  className="model-info-input"
                                />
                              </div>
                              <div className="model-info-item">
                                <label>Table:</label>
                                <input
                                  type="text"
                                  value={model.table}
                                  onChange={(e) => {
                                    const updated = { ...tableData }
                                    updated.semanticModels[modelIndex].table = e.target.value
                                    setTableData(updated)
                                  }}
                                  className="model-info-input"
                                />
                              </div>
                            </div>
                          </div>
                          
                          <EditableTable
                            title="Entities"
                            columns={[
                              { key: 'name', label: 'Name', width: '150px' },
                              { key: 'type', label: 'Type', width: '120px' },
                              { key: 'expr', label: 'Expr', width: '200px', type: 'textarea' },
                              { key: 'label', label: 'Label', width: '150px' },
                              { key: 'description', label: 'Description', width: '250px', type: 'textarea' },
                              { key: 'role', label: 'Role', width: '100px' }
                            ]}
                            data={model.entities || []}
                            onDataChange={(newData) => handleTableDataChange('entities', newData, modelIndex)}
                            onRowClick={(row, index) => handleRowClick('entities', row, index, [
                              { key: 'name', label: 'Name', width: '150px' },
                              { key: 'type', label: 'Type', width: '120px' },
                              { key: 'expr', label: 'Expr', width: '200px', type: 'textarea' },
                              { key: 'label', label: 'Label', width: '150px' },
                              { key: 'description', label: 'Description', width: '250px', type: 'textarea' },
                              { key: 'role', label: 'Role', width: '100px' }
                            ], modelIndex)}
                            onAddRow={(newRow) => handleAddRow('entities', newRow, modelIndex, [
                              { key: 'name', label: 'Name', width: '150px' },
                              { key: 'type', label: 'Type', width: '120px' },
                              { key: 'expr', label: 'Expr', width: '200px', type: 'textarea' },
                              { key: 'label', label: 'Label', width: '150px' },
                              { key: 'description', label: 'Description', width: '250px', type: 'textarea' },
                              { key: 'role', label: 'Role', width: '100px' }
                            ])}
                            onDeleteRow={(rowIndex) => {
                              const updated = { ...tableData }
                              if (!updated.semanticModels[modelIndex].entities) {
                                updated.semanticModels[modelIndex].entities = []
                              }
                              updated.semanticModels[modelIndex].entities = 
                                updated.semanticModels[modelIndex].entities.filter((_, i) => i !== rowIndex)
                              setTableData(updated)
                            }}
                          />
                          
                          <EditableTable
                            title="Dimensions"
                            columns={[
                              { key: 'name', label: 'Name', width: '150px' },
                              { key: 'type', label: 'Type', width: '120px' },
                              { key: 'expr', label: 'Expr', width: '200px', type: 'textarea' },
                              { key: 'label', label: 'Label', width: '150px' },
                              { key: 'description', label: 'Description', width: '300px', type: 'textarea' }
                            ]}
                            data={model.dimensions || []}
                            onDataChange={(newData) => handleTableDataChange('dimensions', newData, modelIndex)}
                            onRowClick={(row, index) => handleRowClick('dimensions', row, index, [
                              { key: 'name', label: 'Name', width: '150px' },
                              { key: 'type', label: 'Type', width: '120px' },
                              { key: 'expr', label: 'Expr', width: '200px', type: 'textarea' },
                              { key: 'label', label: 'Label', width: '150px' },
                              { key: 'description', label: 'Description', width: '300px', type: 'textarea' }
                            ], modelIndex)}
                            onAddRow={(newRow) => handleAddRow('dimensions', newRow, modelIndex, [
                              { key: 'name', label: 'Name', width: '150px' },
                              { key: 'type', label: 'Type', width: '120px' },
                              { key: 'expr', label: 'Expr', width: '200px', type: 'textarea' },
                              { key: 'label', label: 'Label', width: '150px' },
                              { key: 'description', label: 'Description', width: '300px', type: 'textarea' }
                            ])}
                            onDeleteRow={(rowIndex) => {
                              const updated = { ...tableData }
                              if (!updated.semanticModels[modelIndex].dimensions) {
                                updated.semanticModels[modelIndex].dimensions = []
                              }
                              updated.semanticModels[modelIndex].dimensions = 
                                updated.semanticModels[modelIndex].dimensions.filter((_, i) => i !== rowIndex)
                              setTableData(updated)
                            }}
                          />
                          
                          <EditableTable
                            title="Measures"
                            columns={[
                              { key: 'name', label: 'Name', width: '150px' },
                              { key: 'type', label: 'Type', width: '120px' },
                              { key: 'expr', label: 'Expr', width: '200px', type: 'textarea' },
                              { key: 'label', label: 'Label', width: '150px' },
                              { key: 'description', label: 'Description', width: '250px', type: 'textarea' },
                              { key: 'agg', label: 'Agg', width: '100px' }
                            ]}
                            data={model.measures || []}
                            onDataChange={(newData) => handleTableDataChange('measures', newData, modelIndex)}
                            onRowClick={(row, index) => handleRowClick('measures', row, index, [
                              { key: 'name', label: 'Name', width: '150px' },
                              { key: 'type', label: 'Type', width: '120px' },
                              { key: 'expr', label: 'Expr', width: '200px', type: 'textarea' },
                              { key: 'label', label: 'Label', width: '150px' },
                              { key: 'description', label: 'Description', width: '250px', type: 'textarea' },
                              { key: 'agg', label: 'Agg', width: '100px' }
                            ], modelIndex)}
                            onAddRow={(newRow) => handleAddRow('measures', newRow, modelIndex, [
                              { key: 'name', label: 'Name', width: '150px' },
                              { key: 'type', label: 'Type', width: '120px' },
                              { key: 'expr', label: 'Expr', width: '200px', type: 'textarea' },
                              { key: 'label', label: 'Label', width: '150px' },
                              { key: 'description', label: 'Description', width: '250px', type: 'textarea' },
                              { key: 'agg', label: 'Agg', width: '100px' }
                            ])}
                            onDeleteRow={(rowIndex) => {
                              const updated = { ...tableData }
                              if (!updated.semanticModels[modelIndex].measures) {
                                updated.semanticModels[modelIndex].measures = []
                              }
                              updated.semanticModels[modelIndex].measures = 
                                updated.semanticModels[modelIndex].measures.filter((_, i) => i !== rowIndex)
                              setTableData(updated)
                            }}
                          />
                        </div>
                      ))}
                      
                      {tableData.metrics && tableData.metrics.length > 0 && (
                        <div className="metrics-section">
                          <EditableTable
                            title="Metrics"
                            columns={[
                              { key: 'name', label: 'Name', width: '150px' },
                              { key: 'metric_type', label: 'Metric Type', width: '120px' },
                              { key: 'type', label: 'Type', width: '100px' },
                              { key: 'agg', label: 'Agg', width: '120px', type: 'select', options: ['sum', 'sum_boolean', 'count', 'count_distinct', 'avg', 'min', 'max'] },
                              { key: 'measure', label: 'Measure', width: '150px', type: 'select', options: (() => {
                                // 모든 semantic model의 measures를 수집
                                const allMeasures = []
                                tableData.semanticModels.forEach(model => {
                                  if (model.measures && model.measures.length > 0) {
                                    model.measures.forEach(measure => {
                                      allMeasures.push({
                                        value: `${model.name}__${measure.name}`,
                                        label: `${model.name}.${measure.name}`
                                      })
                                    })
                                  }
                                })
                                return allMeasures
                              })() },
                              { key: 'expr', label: 'Expr', width: '200px', type: 'textarea' },
                              { key: 'label', label: 'Label', width: '150px' },
                              { key: 'description', label: 'Description', width: '300px', type: 'textarea' }
                            ]}
                            data={tableData.metrics}
                            onDataChange={(newData) => {
                              const updated = { ...tableData }
                              updated.metrics = newData
                              setTableData(updated)
                            }}
                            onRowClick={(row, index) => handleRowClick('metrics', row, index, [
                              { key: 'name', label: 'Name', width: '150px' },
                              { key: 'metric_type', label: 'Metric Type', width: '120px' },
                              { key: 'type', label: 'Type', width: '100px' },
                              { key: 'agg', label: 'Agg', width: '120px', type: 'select', options: ['sum', 'sum_boolean', 'count', 'count_distinct', 'avg', 'min', 'max'] },
                              { key: 'measure', label: 'Measure', width: '150px', type: 'select', options: (() => {
                                // 모든 semantic model의 measures를 수집
                                const allMeasures = []
                                tableData.semanticModels.forEach(model => {
                                  if (model.measures && model.measures.length > 0) {
                                    model.measures.forEach(measure => {
                                      allMeasures.push({
                                        value: `${model.name}__${measure.name}`,
                                        label: `${model.name}.${measure.name}`
                                      })
                                    })
                                  }
                                })
                                return allMeasures
                              })() },
                              { key: 'expr', label: 'Expr', width: '200px', type: 'textarea' },
                              { key: 'label', label: 'Label', width: '150px' },
                              { key: 'description', label: 'Description', width: '300px', type: 'textarea' }
                            ])}
                            onCellChange={(rowIndex, columnKey, value, newData) => {
                              // agg나 measure가 변경되면 자동으로 name과 expr 업데이트
                              const row = newData[rowIndex]
                              if (columnKey === 'agg' || columnKey === 'measure') {
                                if (row.agg && row.measure) {
                                  // measure는 "모델이름__measure이름" 형식
                                  const measureParts = row.measure.split('__')
                                  if (measureParts.length === 2) {
                                    const [modelName, measureName] = measureParts
                                    // name 자동 생성: total_ 접두사 추가 (expr과 구분하기 위해)
                                    row.name = `total_${row.measure}`
                                    
                                    // expr 자동 생성: AGG(모델이름__measure이름)
                                    const aggUpper = row.agg.toUpperCase()
                                    if (row.agg === 'count_distinct') {
                                      row.expr = `COUNT(DISTINCT ${row.measure})`
                                    } else {
                                      row.expr = `${aggUpper}(${row.measure})`
                                    }
                                  }
                                }
                              }
                              
                              const updated = { ...tableData }
                              updated.metrics = newData
                              setTableData(updated)
                            }}
                            onAddRow={(newRow) => handleAddRow('metrics', newRow, 0, [
                              { key: 'name', label: 'Name', width: '150px' },
                              { key: 'metric_type', label: 'Metric Type', width: '120px' },
                              { key: 'type', label: 'Type', width: '100px' },
                              { key: 'agg', label: 'Agg', width: '120px', type: 'select', options: ['sum', 'sum_boolean', 'count', 'count_distinct', 'avg', 'min', 'max'] },
                              { key: 'measure', label: 'Measure', width: '150px', type: 'select', options: (() => {
                                // 모든 semantic model의 measures를 수집
                                const allMeasures = []
                                tableData.semanticModels.forEach(model => {
                                  if (model.measures && model.measures.length > 0) {
                                    model.measures.forEach(measure => {
                                      allMeasures.push({
                                        value: `${model.name}__${measure.name}`,
                                        label: `${model.name}.${measure.name}`
                                      })
                                    })
                                  }
                                })
                                return allMeasures
                              })() },
                              { key: 'expr', label: 'Expr', width: '200px', type: 'textarea' },
                              { key: 'label', label: 'Label', width: '150px' },
                              { key: 'description', label: 'Description', width: '300px', type: 'textarea' }
                            ])}
                            onDeleteRow={(rowIndex) => {
                              const updated = { ...tableData }
                              updated.metrics = updated.metrics.filter((_, i) => i !== rowIndex)
                              setTableData(updated)
                            }}
                          />
                        </div>
                      )}
                    </div>
                    <div className="editor-footer">
                      <button
                        className="save-button"
                        onClick={handleSave}
                      >
                        💾 저장
                      </button>
                      <button
                        className="cancel-button"
                        onClick={() => {
                          // 원본 파일 내용으로 다시 로드
                          loadFileContent(selectedFile)
                        }}
                      >
                        🔄 되돌리기
                      </button>
                    </div>
                  </div>
                ) : (
                  <div className="editor-wrapper">
                    <textarea
                      value={editedContent}
                      onChange={(e) => setEditedContent(e.target.value)}
                      className="editor-textarea"
                      placeholder="파일 내용을 입력하세요..."
                    />
                    <div className="editor-footer">
                      <button
                        className="save-button"
                        onClick={handleSave}
                      >
                        💾 저장
                      </button>
                      <button
                        className="cancel-button"
                        onClick={() => {
                          setEditedContent(fileContent)
                        }}
                      >
                        🔄 되돌리기
                      </button>
                    </div>
                  </div>
                )}
              </div>
            </>
          ) : (
            <div className="no-file-selected">
              <div className="no-file-content">
                <h2>📄 파일을 선택하세요</h2>
                <p>왼쪽에서 파일을 선택하여 내용을 확인하거나 편집할 수 있습니다.</p>
              </div>
            </div>
          )}
        </div>

        {/* 오른쪽 채팅 패널 */}
        <div className={`chat-side-panel ${showChatPanel ? 'chat-side-panel-open' : ''}`}>
          <div className="chat-side-panel-header">
            <h3>🤖 자동 생성</h3>
            <button
              className="chat-panel-close"
              onClick={() => setShowChatPanel(false)}
            >
              ×
            </button>
          </div>
          <div className="chat-side-panel-content">
            <ChatInterface promptType="yml" />
          </div>
        </div>
      </div>

      {/* 새 파일 생성 모달 */}
      {showDDLModal && (
        <div className="modal-overlay" onClick={() => setShowDDLModal(false)}>
          <div className="modal-content modal-content-large" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h3>새 파일 생성</h3>
              <button
                className="modal-close"
                onClick={() => {
                  setShowDDLModal(false)
                  setDdlText('')
                  setYmlContent('')
                  setModalFileName('')
                  setModalTab('ddl')
                }}
              >
                ×
              </button>
            </div>
            <div className="modal-body">
              {/* 파일 이름 입력 */}
              <div className="modal-form-item">
                <label>
                  파일 이름
                  <input
                    type="text"
                    placeholder="예: 상품기본.yml"
                    value={modalFileName}
                    onChange={(e) => setModalFileName(e.target.value)}
                    className="modal-input"
                  />
                </label>
              </div>

              {/* 탭 */}
              <div className="modal-tabs">
                <button
                  className={`modal-tab ${modalTab === 'ddl' ? 'active' : ''}`}
                  onClick={() => setModalTab('ddl')}
                >
                  📝 DDL에서 생성
                </button>
                <button
                  className={`modal-tab ${modalTab === 'yml' ? 'active' : ''}`}
                  onClick={() => setModalTab('yml')}
                >
                  ✏️ YML 직접 입력
                </button>
              </div>

              {/* 탭 내용 */}
              {modalTab === 'ddl' ? (
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
                      placeholder={`-- ${ddlDialect}\nCREATE TABLE \`project.dataset.table_name\` (\n  \`id\` INTEGER NOT NULL,\n  \`name\` STRING,\n  \`amount\` DECIMAL(10, 2),\n  PRIMARY KEY (\`id\`)\n);`}
                      value={ddlText}
                      onChange={(e) => setDdlText(e.target.value)}
                      rows={15}
                    />
                  </label>
                </div>
              ) : (
                <div className="yml-form">
                  <label>
                    YML 내용 입력
                    <textarea
                      placeholder={`type: semantic_model\nname: 모델명\ntable: 테이블명\n\nentities:\n  - name: entity1\n    type: string\n    expr: column1\n\ndimensions:\n  - name: dim1\n    type: string\n    expr: column2\n\nmeasures:\n  - name: measure1\n    type: number\n    expr: column3\n    agg: sum`}
                      value={ymlContent}
                      onChange={(e) => setYmlContent(e.target.value)}
                      rows={20}
                      className="yml-textarea"
                    />
                  </label>
                </div>
              )}
            </div>
            <div className="modal-footer">
              <button
                className="modal-button-cancel"
                onClick={() => {
                  setShowDDLModal(false)
                  setDdlText('')
                  setYmlContent('')
                  setModalFileName('')
                  setModalTab('ddl')
                }}
              >
                취소
              </button>
              <button
                className="modal-button-submit"
                onClick={modalTab === 'ddl' ? handleCreateFromDDL : handleCreateFromYML}
                disabled={
                  !modalFileName.trim() || 
                  (modalTab === 'ddl' ? (ddlLoading || !ddlText.trim()) : (ymlLoading || !ymlContent.trim()))
                }
              >
                {modalTab === 'ddl' 
                  ? (ddlLoading ? '생성 중...' : 'DDL에서 생성')
                  : (ymlLoading ? '생성 중...' : 'YML 파일 생성')
                }
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Row Edit Modal */}
      {rowEditModal && (
        <div className="modal-overlay" onClick={() => setRowEditModal(null)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h3>
                {rowEditModal.section.charAt(0).toUpperCase() + rowEditModal.section.slice(1)} 
                {rowEditModal.index === -1 ? ' 추가' : ' 편집'}
              </h3>
              <button
                className="modal-close"
                onClick={() => setRowEditModal(null)}
              >
                ×
              </button>
            </div>
            <div className="modal-body">
              <div className="modal-form">
                {rowEditModal.columns.map((col) => (
                  <div key={col.key} className="modal-form-item">
                    <label>
                      {col.label}
                      {col.type === 'textarea' ? (
                        <textarea
                          className="modal-input"
                          value={rowEditModal.data[col.key] || ''}
                          onChange={(e) => handleRowModalChange(col.key, e.target.value)}
                          rows={4}
                        />
                      ) : col.type === 'select' ? (
                        <select
                          className="modal-input"
                          value={rowEditModal.data[col.key] || ''}
                          onChange={(e) => handleRowModalChange(col.key, e.target.value)}
                        >
                          <option value="">선택하세요</option>
                          {col.options && col.options.map(option => (
                            <option key={typeof option === 'string' ? option : option.value} value={typeof option === 'string' ? option : option.value}>
                              {typeof option === 'string' ? option : (option.label || option.value)}
                            </option>
                          ))}
                        </select>
                      ) : (
                        <input
                          type="text"
                          className="modal-input"
                          value={rowEditModal.data[col.key] || ''}
                          onChange={(e) => handleRowModalChange(col.key, e.target.value)}
                        />
                      )}
                    </label>
                  </div>
                ))}
              </div>
            </div>
            <div className="modal-footer">
              <button
                className="modal-button-cancel"
                onClick={() => setRowEditModal(null)}
              >
                취소
              </button>
              <button
                className="modal-button-submit"
                onClick={handleRowSave}
              >
                저장
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

export default YMLManagement

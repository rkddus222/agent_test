import React, { useState } from 'react'
import './EditableTable.css'

function EditableTable({ title, columns, data, onDataChange, onAddRow, onDeleteRow, onCellChange }) {
  const [editingCell, setEditingCell] = useState(null)
  const [editValue, setEditValue] = useState('')

  const handleCellClick = (rowIndex, columnKey) => {
    const cellKey = `${rowIndex}-${columnKey}`
    const column = columns.find(col => col.key === columnKey)
    
    // dropdown 타입인 경우 바로 편집 모드로 진입하지 않고 클릭만 처리
    if (column && column.type === 'select') {
      return
    }
    
    setEditingCell(cellKey)
    setEditValue(data[rowIndex][columnKey] || '')
  }

  const handleCellBlur = () => {
    if (editingCell) {
      const [rowIndex, columnKey] = editingCell.split('-')
      const newData = [...data]
      const oldValue = newData[parseInt(rowIndex)][columnKey]
      newData[parseInt(rowIndex)][columnKey] = editValue
      
      // 값이 변경된 경우에만 콜백 호출
      if (oldValue !== editValue) {
        onDataChange(newData)
      }
      
      setEditingCell(null)
      setEditValue('')
    }
  }

  const handleSelectChange = (rowIndex, columnKey, value) => {
    const newData = data.map((row, idx) => {
      if (idx === rowIndex) {
        return { ...row, [columnKey]: value }
      }
      return row
    })
    
    // onCellChange가 있으면 먼저 호출 (자동 생성 로직 등)
    if (onCellChange) {
      onCellChange(rowIndex, columnKey, value, newData)
    } else {
      onDataChange(newData)
    }
  }

  const handleCellKeyDown = (e) => {
    if (e.key === 'Enter') {
      e.preventDefault()
      handleCellBlur()
    } else if (e.key === 'Escape') {
      setEditingCell(null)
      setEditValue('')
    } else if (e.key === 'Tab') {
      e.preventDefault()
      
      // 현재 셀 저장
      if (editingCell) {
        const [rowIndex, columnKey] = editingCell.split('-')
        const newData = [...data]
        const oldValue = newData[parseInt(rowIndex)][columnKey]
        newData[parseInt(rowIndex)][columnKey] = editValue
        
        if (oldValue !== editValue) {
          onDataChange(newData)
        }
      }
      
      // 다음/이전 셀 찾기
      const [currentRowIndex, currentColumnKey] = editingCell ? editingCell.split('-') : ['0', columns[0].key]
      const currentRow = parseInt(currentRowIndex)
      const currentColIndex = columns.findIndex(col => col.key === currentColumnKey)
      
      let nextRowIndex = currentRow
      let nextColIndex = currentColIndex
      
      if (e.shiftKey) {
        // Shift+Tab: 이전 셀로 이동
        if (nextColIndex > 0) {
          nextColIndex--
        } else {
          // 이전 행의 마지막 셀로
          if (nextRowIndex > 0) {
            nextRowIndex--
            nextColIndex = columns.length - 1
          } else {
            // 첫 번째 셀이면 이동하지 않음
            return
          }
        }
      } else {
        // Tab: 다음 셀로 이동
        if (nextColIndex < columns.length - 1) {
          nextColIndex++
        } else {
          // 다음 행의 첫 번째 셀로
          if (nextRowIndex < data.length - 1) {
            nextRowIndex++
            nextColIndex = 0
          } else {
            // 마지막 셀이면 편집 종료
            setEditingCell(null)
            setEditValue('')
            return
          }
        }
      }
      
      // 다음 셀 편집 시작
      const nextCellKey = `${nextRowIndex}-${columns[nextColIndex].key}`
      setEditingCell(nextCellKey)
      setEditValue(data[nextRowIndex][columns[nextColIndex].key] || '')
    }
  }

  const handleDelete = (rowIndex) => {
    if (window.confirm('이 행을 삭제하시겠습니까?')) {
      const newData = data.filter((_, index) => index !== rowIndex)
      onDataChange(newData)
    }
  }

  return (
    <div className="editable-table-container">
      <div className="editable-table-header">
        <h3 className="editable-table-title">{title}</h3>
        {onAddRow && (
          <button
            className="add-row-button"
            onClick={() => {
              const newRow = {}
              columns.forEach(col => {
                newRow[col.key] = ''
              })
              onAddRow(newRow)
            }}
            title="행 추가"
          >
            ➕ 추가
          </button>
        )}
      </div>
      
      {data.length === 0 ? (
        <div className="editable-table-empty">
          <p>데이터가 없습니다.</p>
          {onAddRow && (
            <button
              className="add-first-row-button"
              onClick={() => {
                const newRow = {}
                columns.forEach(col => {
                  newRow[col.key] = ''
                })
                onAddRow(newRow)
              }}
            >
              첫 번째 행 추가
            </button>
          )}
        </div>
      ) : (
        <div className="editable-table-wrapper">
          <table className="editable-table">
            <thead>
              <tr>
                {columns.map(col => (
                  <th key={col.key} style={{ width: col.width }}>
                    {col.label}
                  </th>
                ))}
                {onDeleteRow && <th style={{ width: '60px' }}>작업</th>}
              </tr>
            </thead>
            <tbody>
              {data.map((row, rowIndex) => (
                <tr key={row.id || rowIndex}>
                  {columns.map(col => {
                    const cellKey = `${rowIndex}-${col.key}`
                    const isEditing = editingCell === cellKey
                    const cellValue = row[col.key] || ''

                    return (
                      <td key={col.key}>
                        {col.type === 'select' ? (
                          <select
                            className="editable-cell-select"
                            value={cellValue}
                            onChange={(e) => handleSelectChange(rowIndex, col.key, e.target.value)}
                          >
                            <option value="">선택하세요</option>
                            {col.options && col.options.map(option => (
                              <option key={typeof option === 'string' ? option : option.value} value={typeof option === 'string' ? option : option.value}>
                                {typeof option === 'string' ? option : (option.label || option.value)}
                              </option>
                            ))}
                          </select>
                        ) : isEditing ? (
                          <input
                            type="text"
                            className="editable-cell-input"
                            value={editValue}
                            onChange={(e) => setEditValue(e.target.value)}
                            onBlur={handleCellBlur}
                            onKeyDown={handleCellKeyDown}
                            autoFocus
                          />
                        ) : (
                          <div
                            className="editable-cell"
                            onClick={() => handleCellClick(rowIndex, col.key)}
                            title="클릭하여 편집"
                          >
                            {cellValue || <span className="empty-cell-placeholder">-</span>}
                          </div>
                        )}
                      </td>
                    )
                  })}
                  {onDeleteRow && (
                    <td>
                      <button
                        className="delete-row-button"
                        onClick={() => handleDelete(rowIndex)}
                        title="행 삭제"
                      >
                        🗑️
                      </button>
                    </td>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

export default EditableTable


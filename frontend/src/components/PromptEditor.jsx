import React, { useState, useEffect } from 'react'
import axios from 'axios'
import './PromptEditor.css'

function PromptEditor() {
  const [prompt, setPrompt] = useState('')
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)

  useEffect(() => {
    loadPrompt()
  }, [])

  const loadPrompt = async () => {
    try {
      const response = await axios.get('/api/prompt')
      setPrompt(response.data.prompt)
    } catch (error) {
      console.error('프롬프트 로드 실패:', error)
    } finally {
      setLoading(false)
    }
  }

  const handleSave = async () => {
    setSaving(true)
    try {
      await axios.post('/api/prompt', { prompt })
      alert('System prompt saved!')
    } catch (error) {
      alert('프롬프트 저장 실패: ' + error.response?.data?.detail || error.message)
    } finally {
      setSaving(false)
    }
  }

  if (loading) {
    return <div className="prompt-editor-loading">로딩 중...</div>
  }

  return (
    <div className="prompt-editor">
      <h2>System Prompt Editor</h2>
      <textarea
        className="prompt-textarea"
        value={prompt}
        onChange={(e) => setPrompt(e.target.value)}
        placeholder="Edit the system prompt here..."
      />
      <button
        className="prompt-save-button"
        onClick={handleSave}
        disabled={saving}
      >
        {saving ? '저장 중...' : 'Save Prompt'}
      </button>
    </div>
  )
}

export default PromptEditor



import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, act } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import ChatInterface from '../ChatInterface.jsx'

class MockWebSocket {
  static CONNECTING = 0
  static OPEN = 1
  static CLOSING = 2
  static CLOSED = 3

  static instances = []

  constructor(url) {
    this.url = url
    this.readyState = MockWebSocket.OPEN
    this._onopen = null
    this.onmessage = null
    this.onerror = null
    this.onclose = null
    MockWebSocket.instances.push(this)
  }

  set onopen(fn) {
    this._onopen = fn
    // connectWebSocket에서 핸들러가 할당되는 순간, 즉시 open 이벤트를 발생시켜
    // 불필요한 타이머 경로(setTimeout)를 최소화하고 act 경고를 줄입니다.
    this._onopen?.()
  }

  get onopen() {
    return this._onopen
  }

  send() {}

  close() {
    this.readyState = MockWebSocket.CLOSED
    this.onclose?.()
  }
}

describe('ChatInterface - 처리 중 UI', () => {
  beforeEach(() => {
    MockWebSocket.instances = []
    vi.stubGlobal('WebSocket', MockWebSocket)
    vi.useFakeTimers()
  })

  it('전송 후 처리 중에는 입력창/버튼이 비활성화되고 "처리 중..."이 표시된다', async () => {
    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime })
    render(<ChatInterface />)

    const textarea = screen.getByPlaceholderText('모델에서 변경하고 싶은 내용을 입력하세요')
    await user.type(textarea, '테스트 메시지')

    const sendButton = screen.getByRole('button', { name: '전송' })
    await user.click(sendButton)

    expect(textarea).toBeDisabled()
    expect(screen.getByRole('button', { name: /처리 중/ })).toBeDisabled()

    // handleSend 내부 타이머(setTimeout)로 인해 발생하는 state update를 act로 정리
    await act(async () => {
      await vi.runAllTimersAsync()
    })
  })
})



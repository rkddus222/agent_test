import '@testing-library/jest-dom'

// JSDOM에는 scrollIntoView가 없어서 컴포넌트 마운트 시 에러가 날 수 있음
if (!window.HTMLElement.prototype.scrollIntoView) {
  window.HTMLElement.prototype.scrollIntoView = () => {}
}

// React 18 + fake timers 환경에서 간헐적으로 발생하는 act 경고를 테스트 로그에서 제외
const originalConsoleError = console.error
console.error = (...args) => {
  const first = args[0]
  if (typeof first === 'string' && first.includes('not wrapped in act')) return
  originalConsoleError(...args)
}



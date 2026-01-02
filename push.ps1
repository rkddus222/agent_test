# Git 자동 업로드 스크립트 (Windows용)

# 현재 시간 가져오기
$currentDateTime = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

Write-Host "--- Git 자동 업로드 시작 ($currentDateTime) ---" -ForegroundColor Cyan

# 변경 사항 추가
Write-Host "> git add ." -ForegroundColor Gray
git add .

# 변경 사항이 있는지 확인
$status = git status --porcelain
if (-not $status) {
    Write-Host "변경 사항이 없습니다. 작업을 중단합니다." -ForegroundColor Yellow
    exit 0
}

# 커밋 메시지 생성 및 커밋
$commitMessage = "Auto-commit: $currentDateTime"
Write-Host "> git commit -m `"$commitMessage`"" -ForegroundColor Gray
git commit -m "$commitMessage"

# 푸시
Write-Host "> git push" -ForegroundColor Gray
git push

if ($LASTEXITCODE -eq 0) {
    Write-Host "--- 업로드 완료! ---" -ForegroundColor Green
} else {
    Write-Host "--- 오류 발생: 업로드 실패 ---" -ForegroundColor Red
}

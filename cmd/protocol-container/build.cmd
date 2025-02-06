@echo off
set CGO_ENABLED=0
set GOOS=linux
set GOARCH=amd64

go build -o protocol-container main.go

if %errorlevel% neq 0 (
    echo Build failed.
    exit /b %errorlevel%
)

echo Build successful.
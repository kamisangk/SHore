package ui

import (
	"embed"
	"io/fs"
)

// Dist 保存内置前端构建产物。
//
// 使用 all:dist 以包含 Vite 生成的 `_plugin-*.js` 等以下划线开头的文件。
//
//go:embed all:dist
var Dist embed.FS

func DistFS() (fs.FS, error) {
	return fs.Sub(Dist, "dist")
}

# GoBatch

[![GoDoc Reference](https://godoc.org/github.com/chararch/gobatch?status.svg)](http://godoc.org/github.com/chararch/gobatch)
[![Go Report Card](https://goreportcard.com/badge/github.com/chararch/gobatch)](https://goreportcard.com/report/github.com/chararch/gobatch)
[![MIT license](https://img.shields.io/badge/license-MIT-brightgreen.svg)](https://opensource.org/licenses/MIT)

中文 | [English](README.md)

GoBatch是一款Go语言的批处理框架，类似于Java的Spring Batch。对于熟悉Spring Batch的用户来说，GoBatch非常容易上手。

## 功能

- 模块化构建批处理应用
- 支持串行和并行处理
- 内置文件处理组件
- 提供任务和步骤执行的监听器
- 易于扩展

## 安装

```shell
go get -u github.com/chararch/gobatch
```

## 快速开始
1. 设置数据库并使用提供的schema创建必要的表。
2. 使用GoBatch编写批处理代码。
3. 运行批处理任务。
有关详细文档和示例，请参阅[项目文档](https://chararch.github.io/gobatch-doc)。

## 许可证
此项目根据MIT许可证授权。有关详细信息，请参阅[LICENSE](LICENSE)文件。

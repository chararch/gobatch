# GoBatch

[![GoDoc Reference](https://godoc.org/github.com/chararch/gobatch?status.svg)](http://godoc.org/github.com/chararch/gobatch)
[![Go Report Card](https://goreportcard.com/badge/github.com/chararch/gobatch)](https://goreportcard.com/report/github.com/chararch/gobatch)
[![MIT license](https://img.shields.io/badge/license-MIT-brightgreen.svg)](https://opensource.org/licenses/MIT)

English | [中文](README_zh.md)

GoBatch is a batch processing framework in Go, similar to Spring Batch in Java. It is designed to be easy to use for those familiar with Spring Batch.

## Features

- Modular construction for batch applications
- Support for serial and parallel processing
- Built-in file processing components
- Listeners for job and step execution
- Easy to extend

## Installation

```shell
go get -u github.com/chararch/gobatch
```

## Quick Start
1. Set up a database and create necessary tables using the provided schema.
2. Write your batch processing code using GoBatch.
3. Run your batch jobs.
For detailed documentation and examples, please refer to the [project documentation](https://chararch.github.io/gobatch-doc) .

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
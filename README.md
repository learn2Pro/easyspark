# 1. easyspark inspired by [60824](http://nil.csail.mit.edu/6.824/2022/labs/lab-mr.html)

## 1.1 lab1
### commands
```shell
cd ~/export/easyspark/src
go build -race -buildmode=plugin ./mrapps/wc.go
rm mr-out*
go run -race main/mrsequential.go wc.so main/pg*.txt

or

go build -buildmode=plugin ./mrapps/wc.go
go run main/mrsequential.go wc.so main/pg*.txt
```

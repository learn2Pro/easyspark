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

or

go run -race mrcoordinator.go pg-*.txt
go build -buildmode=plugin ../mrapps/wc.go
go run mrworker.go wc.so
cat mr-out-* | sort | more
result will be like 
A 509
ABOUT 2
ACT 8
...

or 

bash test-mr.sh
result will be like
*** Starting wc test.
--- wc test: PASS
*** Starting indexer test.
--- indexer test: PASS
*** Starting map parallelism test.
--- map parallelism test: PASS
*** Starting reduce parallelism test.
--- reduce parallelism test: PASS
*** Starting crash test.
--- crash test: PASS
*** PASSED ALL TESTS
$

```
# 2.1 lab2a
```shell
go test -run 2A -race
```

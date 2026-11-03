module github.com/mdb-official/mdb-go

go 1.21

require github.com/mdb-official/mdb-native v0.0.0

// mdb-native 就在同一个仓库里：用本地路径替换，既不需要网络、
// 也不需要 go.sum（被 replace 到本地路径的模块没有校验和）。
replace github.com/mdb-official/mdb-native => ../mdb-native

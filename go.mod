module github.com/fogfish/segment

go 1.20

require (
	github.com/fogfish/guid/v2 v2.0.4
	github.com/fogfish/skiplist v0.14.1
)

require github.com/fogfish/golem/trait v0.2.0 // indirect

replace github.com/fogfish/skiplist => ../skiplist

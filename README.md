# mu

**Warning:** *Here be dragons, and lots of them.  `mu` is nowhere near
finished, production ready or guaranteed (or even likely) to not mess
up your data.  It is part research project, part utility in use at
(really) tiny scale.  You're welcome to look around and have fun (and
give feedback), but don't expect that it won't mess up things.*

`mu` is a database that stores tuples, with a lightweight schema mechanism
and a design that will support a powerful query language in the future.

It is inspired by [Datomic](http://datomic.com) and [DataScript](https://github.com/tonsky/datascript)
and follows Datomic's API closely.

`mu` is intended to be used in places where `sqlite` is used, but where a
more flexible schema and (in the future) more powerful queries and/or immutable
storage are desired.

## Quickstart

Using `mu` currently means using it's Go API.  Here's a small example:

```go
import (
    "fmt"
    "github.com/heyLu/mu"
)

func main() {
    conn, err := mu.Connect("file://posts.db")
    db, _ := conn.Db()

    iter := db.Eavt().Datoms()
    for datom := iter.Next(); datom != nil; datom = iter.Next() {
        fmt.Println(datom)
    }
}
```

For a more detailed example, take a look at the [`notes` example application](./cmd/notes/notes.go).

## Documentation

For now, you need to be familar with both Go and Datomic to be able to use
`mu`.  In the future, we hope to provide a REST API for use with other
languages, detailed documentation for both the Go API and the commandline
interface.

Note that the following are *not* implemented right now.  (Which may be
considered as "the interesting parts" of Datomic.)

- queries (in progress.)
- pull api (planned.)
- indexing (in progress.  as of now, we can only read segmented indexes.)
- proper schema support (in progress.  attribute changes are currently
    not checked for correctness.)

## Contributing

Thanks for your interest!  Please take the following things into consideration:

- all contributors are expected to follow our [Code of Conduct]
- some areas for improvements:
    - compatibility with Datomics API
    - documentation and examples
    - extensive testing
    - performance

`mu` is currently distributed under the [`MIT` license](./LICENSE).  By
contributing to it you agree to license your contribution under the same
license.

## Thanks

First and foremost, a very big thank you to Rich Hickey and everyone who
has worked on Datomic.  This wouldn't be possible without you, thank you
for envisioning and implementing a database that works in tandem with
functional programming and immutable data structures.

In no particular order:

- [DataScript](https://github.com/tonsky/datascript), which provides both
    inspiration and the BTSet implementation on which ours is based
- [Go](https://golang.org) and it's creators, which provides us with a
    deeply pragmatic language that's a joy to work with.

## FAQ

- Why?  Both Datomic and DataScript could be used instead.

    `mu` is intended to be used in situations were neither Datomic nor
    DataScript are appropriate.  In particular, Datomic has relatively
    high memory requirements, which make it unsuitable for commandline
    tools like the `notes` example.  DataScript, on the other hand, is
    written in JavaScript and intended for use in web applications and
    does not support some Datomic features that we'd like to have, notably
    the reified schema and history, which we hope to add in the future.
- When should I use it?

    First of all, if you're already happy with Datomic and/or DataScript,
    you shouldn't.  `mu` is not intended to replace them.

    However, if you are in a situation where you would want to use
    something like sqlite, but want "more" (indexes, flexible schema,
    api, history, and in the future, powerful queries), then you
    might want to have a closer look.

    For now, you don't want to use it with large data sets, or anything
    non-experimental.  In general, it is usable in "personal scale"
    projects.
- Why is it written in Go?

    There is no definitive reason for that, but it surely helped that it
    compiles fast, supports standalone binaries and allows for use in
    commandline and other short running applications.  It also helped
    that I was already familar with it and have recently written a few
    smallish applications in it.

    Alternative languages I've considered using are Rust and Ocaml,
    but I'm not familar to use either of those while also figuring out
    how to write `mu` itself.  Aside from that, the main benefit would
    be to support access to the API from C via a FFI interface.

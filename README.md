# mu

**Warning:** *Here be dragons, and lots of them.  `mu` is nowhere near
finished, production ready or guaranteed (or even likely) to not mess
up your data.  It is part research project, part utility in use at
(really) tiny scale.  You're welcome to look around and have fun (and
give feedback), but don't expect that it won't mess up things.*

`mu` is a database that stores tuples, with a lightweight schema mechanism
and a design that will support custom query languages and immutable storage
in the future.

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
    "net/url"
)

func main() {
    u, _ := url.Parse("file://posts.db")
    conn, err := mu.Connect(u)
    db, _ := conn.Db()

    iter := db.Datoms()
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

- queries (planned.  for now you will have to use the raw index for queries)
- segmented index (in progress)
- log (in progress)
- history (not planned for now.  the log will store the data, so we can
    support this later.  the database values are still immutable, though.)
- proper schema support (in progress.  support for checking attribute types,
    cardinality many attributes and a few other things is not implemented
    yet.)

## Contributing

Thanks for your interest!  Please take the following things into consideration:

- all contributors are expected to follow our [Code of Conduct]
- some areas for improvements:
    - compatibility with Datomic's API
    - documentation and examples
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
- Why is it written in Go?

    No particular reason, except that Go compiles fast, supports standalone
    binaries and allows for use in commandline and other short running
    applications.  I've thought about porting it to Rust, so that it can
    export a C-style API that can be used via FFI from many languages, but
    I'm not familar enough with it to do so while also trying to figure
    out how to write `mu` itself.

# pgmigrate
A Go library and app to migrate Postgresql databases.

(Not production ready; do not use).

## Why?

Why yet another one?

I needed something simple, embeddable that does what I need.

That means PostgreSQL (and only PostgreSQL), pgx, a linear sequence of up/down sql scripts
(if you want feature patches there's squitch). Transaction based, so there's no such thing
as a partially applied patch.

## How?

Create a directory with patch files in it, named <number>_<description>.up.sql and
<number>_<description>.down.sql.


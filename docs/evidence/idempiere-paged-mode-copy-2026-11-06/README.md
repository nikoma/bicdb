# Original application baseline: verified storage-mode copy

An offline cold clone of the qualified original Oracle-schema baseline copied
successfully from embedded storage to paged storage: **923 native collections,
188,817 native records and 2,087 native indexes**. The CLI exited zero after
closing and reopening the target and comparing every stored record checksum,
logical collection contract and secondary-index definition, then verifying
index contents against target rows. These native totals include internal
catalog/storage collections; they are not counts of application test passes.

The [exact artifact and evidence hashes](summary.json) identify CLI source
`e5babd1` and the source baseline/export. The protected baseline's file contents
were independently rehashed after the copy and remain unchanged. Recovery and
checkpoint operations were restricted to the cold clone and its new target.

A resource guard interrupted an additional standalone integrity check after
the mode-copy command completed. The completed copy is retained and the
remaining checks are queued for continuation. This checkpoint does not yet
qualify the paged baseline's SQL values, owners, FK/orphan inventory, views,
monitoring privileges or original application behavior. UI/workflow acceptance,
production qualification and the full Oracle programme remain open.

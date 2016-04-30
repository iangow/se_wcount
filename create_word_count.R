library("RPostgreSQL")

pg <- dbConnect(PostgreSQL())

if (!dbExistsTable(pg, c("jpark", "word_count"))) {
    dbGetQuery(pg, "
        DROP TABLE IF EXISTS jpark.word_count;

        CREATE TABLE jpark.word_count
        (
          file_name text,
          last_update timestamp without time zone,
          context text,
          word_count integer
        );

        CREATE INDEX ON jpark.word_count (file_name, last_update);

        ALTER TABLE jpark.word_count OWNER TO jpark;")
}

file_list <- dbGetQuery(pg, "
    SET work_mem='3GB';

    SELECT DISTINCT file_name, last_update
    FROM streetevents.calls AS a
    WHERE call_type=1 AND
        (file_name, last_update) NOT IN
            (SELECT file_name, last_update
             FROM  jpark.word_count)
    LIMIT 100")

rs <- dbDisconnect(pg)

addData <- function(file_name) {
    library("RPostgreSQL")
    sql <- paste(readLines("create_word_count.sql"), collapse="\n")

    pg <- dbConnect(PostgreSQL())
    dbGetQuery(pg, sprintf(sql, file_name, file_name))
    dbDisconnect(pg)
}

library(parallel)
system.time(res <- unlist(mclapply(file_list$file_name, addData, mc.cores=8)))


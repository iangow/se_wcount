DELETE FROM jpark.word_count WHERE file_name='%s';

INSERT INTO jpark.word_count
    (file_name, last_update, context, word_count)
SELECT file_name, last_update, context,
    sum(word_count(speaker_text)) AS word_count
FROM streetevents.speaker_data
WHERE file_name='%s'
GROUP BY file_name, last_update, context;

# CSV2Avro

Конвертирует файл в формате CSV в файл в формате Avro.

Распознаёт типы Int, Long и Double в файле CSV.

Сохраняет схему в файл .avsc

## Пример
- cd example
- java -Dconfig.file=application.conf -jar ../target/scala-2.13/CSV2Avro-assembly-1.0.jar
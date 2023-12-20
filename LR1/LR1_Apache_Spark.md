#Лабораторная работа 1

Работа начинается с инициализации сессии и загрузки необходимых данных: 
ссылка на изображение

##1. Найти велосипед с максимальным временем пробега.
  Для нахождения велосипеда с максимальным временем пробега для начала выполняется группировка данных
`trip.groupBy("bike_id")` о поездках по уникальным значениям в столбце "bike_id". Это позволяет получить 
информацию о всех поездках, связанных с конкретным велосипедом. 
  После группировки применяется функция 'max("duration")', которая позволяет вычислить максимальное значение 
столбце "duration" для каждой группы. Затем результат получает псевдоним "max_duration".
  Далее используется метод orderBy для сортировки результатов в порядке убывания времени пробега.
`col("max_duration").desc()` указывает, что происходит сортировка по столбцу "max_duration" в порядке убывания.
  `.first()` - выбирает первую строку в отсортированных данных. Таким образом, получается велосипед с максимальным временем пробега.

ССылка на изображение с кодом








Найти наибольшее геодезическое расстояние между станциями.
Найти путь велосипеда с максимальным временем пробега через станции.
Найти количество велосипедов в системе.
Найти пользователей потративших на поездки более 3 часов.
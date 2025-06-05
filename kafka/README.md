Проект виконує наступні кроки:
1. Завантажує дані з CSV-файлу до бази даних PostgreSQL.
2. Завантажує дані з PostgreSQL до теми Kafka (`coffee-products`).
3. Обробляє потік даних за допомогою Kafka Streams:
   - Фільтрує записи про напої, у яких більше 200 ккал.
   - Розділяє записи на три гілки за типом молока: без молока, з кокосовим молоком, інше.
   - Зберігає відфільтровані та розділені результати в окремих темах Kafka.
4. Споживає дані з результуючих тем для демонстрації оброблених даних.

Вимоги

Для запуску проекту вам потрібні:
- Java 8 або вище
- Apache Maven
- Docker та Docker Compose

Запуск інфраструктури Docker

Переконайтеся, що у вас встановлені Docker та Docker Compose.
У кореневій директорії проекту (`kafka3`) виконайте команду:


docker-compose up -d


Ця команда запустить наступні контейнери:
- PostgreSQL (порт 5430): база даних для зберігання початкових даних.
- Zookeeper (порт 2181): необхідний для роботи Kafka.
- Kafka (порт 9092): брокер повідомлень Kafka.
- Kafka UI (порт 8080): веб-інтерфейс для перегляду тем та повідомлень Kafka.

Перевірити стан запущених контейнерів можна командою:


docker ps


Компіляція проекту

Перейдіть до директорії `kafka` та скомпілюйте проект за допомогою Maven:


cd kafka
mvn clean package


Ця команда збере проект у виконуваний JAR-файл з усіма залежностями (`target/kafka-1.0-SNAPSHOT-jar-with-dependencies.jar`).

Створення Kafka тем

Теми (`coffee-products`, `no_milk_drinks`, `coconut_milk_drinks`, `other_milk_drinks`) будуть створені автоматично при першому запуску відповідних компонентів Kafka.

Завантаження даних

1. Завантаження даних з CSV до PostgreSQL:
   Виконайте наступну команду з директорії `kafka`:

   java -cp target/kafka-1.0-SNAPSHOT-jar-with-dependencies.jar lab1.CsvToPostgresLoader
   Це завантажить дані з файлу `starbucks.csv` (переконайтеся, що він знаходиться в директорії `kafka`) до бази даних PostgreSQL.

2. Завантаження даних з PostgreSQL до Kafka:
   Відкрийте новий термінал, перейдіть до директорії `kafka` та запустіть Producer, який зчитає дані з PostgreSQL та надішле їх до теми `coffee-products`:


   java -cp target/kafka-1.0-SNAPSHOT-jar-with-dependencies.jar lab1.KafkaProducerFromDB

Запуск додатків Kafka

Відкрийте окремі термінали для кожного з наступних компонентів і запустіть їх (з директорії `kafka`):

1. Запуск Kafka Streams додатку:
   Цей додаток обробляє дані з теми `coffee-products`.


   java -cp target/kafka-1.0-SNAPSHOT-jar-with-dependencies.jar lab1.KafkaStreamsApp
   

2. Запуск Consumer для перегляду результатів:
   Цей додаток споживає дані з тем результатів (`no_milk_drinks`, `coconut_milk_drinks`, `other_milk_drinks`) і виводить їх у консоль.


   java -cp target/kafka-1.0-SNAPSHOT-jar-with-dependencies.jar lab1.KafkaStreamResultsConsumer
   

Перевірка результатів

Ви можете переглянути повідомлення у темах Kafka за допомогою Kafka UI. Відкрийте у браузері:


http://localhost:8080


Також можна перевірити дані безпосередньо в базі даних PostgreSQL за допомогою Docker:

- Перевірка загальної кількості напоїв у таблиці `coffee_products`:

  docker exec -it kafka3-postgres-1 psql -U postgres_user -d postgres_db -c "SELECT COUNT(*) FROM coffee_products;"
  

- Перевірка загальної кількості напоїв з калоріями > 200 в базі:
  
  docker exec -it kafka3-postgres-1 psql -U postgres_user -d postgres_db -c "SELECT COUNT(*) FROM coffee_products WHERE calories > 200;"
  

- Розподіл за категоріями молока (для напоїв > 200 ккал):
  
  docker exec -it kafka3-postgres-1 psql -U postgres_user -d postgres_db -c "SELECT CASE WHEN milk = 0 THEN 'Без молока' WHEN milk = 5 THEN 'Кокосове молоко' ELSE 'Інші типи молока' END AS категорія, COUNT(*) FROM coffee_products WHERE calories > 200 GROUP BY категорія ORDER BY категорія;"
  
  (Примітка: Цей запит дасть розподіл у БД, який має відповідати кількості повідомлень у вихідних темах Kafka після обробки Kafka Streams).

Очищення бази даних PostgreSQL

Ви можете очистити таблицю `coffee_products` в базі даних PostgreSQL за допомогою наступної команди (з директорії `kafka`):


java -cp target/kafka-1.0-SNAPSHOT-jar-with-dependencies.jar lab1.DatabaseCleaner


Зупинка проекту

Для зупинки всіх запущених Docker контейнерів виконайте команду в кореневій директорії проекту (`kafka3`):


docker-compose down



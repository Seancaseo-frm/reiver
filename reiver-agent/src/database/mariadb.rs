// MariaDB collector - reuses MySQL code since MariaDB is MySQL-compatible
// MariaDB uses the same performance_schema as MySQL

use crate::database::mysql::MySQLCollector;

// MariaDB uses the same implementation as MySQL since they're compatible
// The only difference is the database type string ("mariadb" vs "mysql")
// We can reuse the MySQL collector directly by using a type alias

pub type MariaDBCollector = MySQLCollector;


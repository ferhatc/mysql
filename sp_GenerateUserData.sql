/*
	* Author: Ferhat Can <ferhat.can@gmail.com>
	*
	* Generate user data for the medium article. Please do not run it on producton servers.
	* pcount: The number of items to be inserted into the users table, limited to values between 1_000 and 10_000_000
	* types of the users will not be homogenous, we will have 5 moderators, 4 managers and 1 admin for every 100_000 normal users.
	* Note that the password is an md5 hash and you should use more powerful hashes like Bcrypt in production, which is not supported natively by MySQL.
*/
DELIMITER $$

DROP PROCEDURE IF EXISTS sp_GenerateUserData;

CREATE PROCEDURE sp_GenerateUserData(IN pcount INT)
my_sp: BEGIN

	DECLARE c_batch_count INT DEFAULT 1000;
	DECLARE c_range_count INT DEFAULT 100000;
	DECLARE c_test_user_freq INT DEFAULT 10000; #On the average 1/10000 of the users will be set as test user.
	DECLARE c_min_param INT DEFAULT 1000;
	DECLARE c_max_param INT DEFAULT 10000000;
	
	DECLARE l_record_count INT;
	DECLARE l_insert_count INT DEFAULT 0;	
	DECLARE l_bin_log_stat TINYINT(1);
	
	SELECT @@SESSION.sql_log_bin INTO l_bin_log_stat;
	#Disable binlogs for faster writes for this session.
	SET @@SESSION.sql_log_bin=0;
	
	#Create the table if not already created. 
	CREATE TABLE IF NOT EXISTS `scratch`.`users` (
		id INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
		first_name VARCHAR(50) NOT NULL COLLATE utf8mb4_general_ci,
		sur_name VARCHAR(50) NOT NULL COLLATE utf8mb4_general_ci,
		email VARCHAR(100) NOT NULL COLLATE utf8mb4_general_ci,
		pass CHAR(32) NOT NULL COLLATE utf8mb4_general_ci COMMENT 'MD5 hashed, use better hashing in producton',
		type TINYINT(4) NOT NULL DEFAULT 1 COMMENT '1 => Normal, 2 => Manager, 3 => Moderator, 4 => Admin',
		is_test TINYINT(1) NOT NULL DEFAULT 0 COMMENT '0 => Real, 1 => Test',
		last_login TIMESTAMP NULL DEFAULT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4 COLLATE utf8mb4_general_ci;

	#Some input validations.
	IF pcount > c_max_param THEN
		SELECT 'The upper limit for every run is 10M, no operation will be performed!' AS Status;
		LEAVE my_sp;
	ELSEIF pcount < c_min_param THEN
		SELECT 'The lower limit for every run is 1000, no operation will be performed!' AS Status;
		LEAVE my_sp;
	END IF;
	
	SELECT COUNT(*) INTO l_record_count FROM `scratch`.`users`;
	SELECT '', '' INTO @values, @insert;
	SELECT 1, 1 INTO @i, @j;
	SET @multiplier = 1;
	
	WHILE @i <= pcount DO
		SET @string = MD5(CONCAT(pcount, @i, NOW(3), RAND()));
		SET @name = LEFT(@string, 8);
		SET @surname = RIGHT(@string, 8);
		#The email domains will be 3 characters extracted from @string so that we have some common domains
		SET @email = CONCAT(@i + l_record_count, '@', MID(@string, 16,3),'.com');
		SET @type = 1;
		SET @last_login = 'NULL';
		SET @is_test = 0;
		#To have different user types to match real cases.
		IF @i > @multiplier * c_range_count THEN
			IF @j <= 5 THEN
				SET @type = 3;
				SET @j = @j+1;
			ELSEIF @j = 10 THEN
				SELECT 4, 1, @multiplier + 1 INTO @type, @j, @multiplier;
			ELSE
				SET @type = 2;
				SET @j = @j+1;
			END IF;
		END IF;
		IF LEFT(@string, 1) = 'a' THEN
			SET @last_login = CURRENT_TIMESTAMP();
		END IF;
		IF RAND()*c_test_user_freq <= 1 THEN
			SET @is_test = 1;
		END IF;
		#Instead of performing single inserts, we make use of the multiple inserts which creates extensive insert boost at the cost of running dynamic SQL.
		SET @values = CONCAT("('",@name,"','", @surname,"','",  @email, "','", MD5(CONCAT(@name, @surname)), "',", @type,",", @is_test, ",", IF(@last_login != 'NULL', CONCAT("'", @last_login, "'"),@last_login), ")");
		IF @insert = '' THEN
			SET @insert = @values;
		ELSE
			SET @insert = CONCAT(@values, ",", @insert);	
		END IF;
		#We make the inserts to be batched by c_batch_count constant to increase the insertion speed
		IF @i % c_batch_count = 0 THEN
			SET @SQL = CONCAT("INSERT INTO `scratch`.`users` (first_name, sur_name, email, pass, type, is_test, last_login) VALUES ",@insert, ";");
			PREPARE stmt FROM @SQL;
			EXECUTE stmt;
			SET l_insert_count = l_insert_count + ROW_COUNT();
			DEALLOCATE PREPARE stmt;
			SET @insert = '';
		END IF;
		SET @i = @i + 1;
	END WHILE;
	
	#If there are any leftovers that are not bacthed yet (pcount is not evenly divisible by c_batch_count), we need to process them
	IF @insert != '' THEN
		SET @SQL = CONCAT("INSERT INTO `scratch`.`users` (first_name, sur_name, email, pass, type, is_test, last_login) VALUES ",@insert, ";");
		PREPARE stmt FROM @SQL;
		EXECUTE stmt;
		SET l_insert_count = l_insert_count + ROW_COUNT();
		DEALLOCATE PREPARE stmt;
	END IF;
	
	
	SET @@SESSION.sql_log_bin=l_bin_log_stat;
	
	SELECT CONCAT_WS(' ', FORMAT(l_insert_count, 0), 'rows inserted') AS Status;

END $$
		
DELIMITER ;
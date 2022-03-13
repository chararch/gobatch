
CREATE TABLE t_trade (
     id bigint NOT NULL AUTO_INCREMENT,
     trade_no varchar(64) NOT NULL,
     account_no varchar(60) NOT NULL,
     type varchar(20) NOT NULL,
     amount decimal(10,2) NOT NULL,
     terms int(11) NOT NULL,
     interest_rate decimal(10,6) NOT NULL,
     trade_time datetime NOT NULL,
     status varchar(10) NOT NULL,
     create_time datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
     update_time timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
     PRIMARY KEY (id),
     UNIQUE KEY uniq_trade_no (trade_no)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE t_repay_plan (
      id bigint NOT NULL AUTO_INCREMENT,
      account_no varchar(60) NOT NULL,
      loan_no varchar(64) NOT NULL,
      term int(11) NOT NULL,
      principal decimal(10,2) NOT NULL,
      interest decimal(10,2) NOT NULL,
      init_date date NOT NULL,
      repay_date date NOT NULL,
      repay_state varchar(10) NOT NULL,
      create_time datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
      update_time timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
      PRIMARY KEY (id),
      UNIQUE KEY uniq_loan_no_term (loan_no, term)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

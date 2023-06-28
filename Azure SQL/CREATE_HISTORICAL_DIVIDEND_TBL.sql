CREATE TABLE curated.history_dividends(
    symbol varchar(255),
    date date,
    label varchar(255),
    adjDividend decimal(38,6),
    dividend decimal(38,6),
    recordDate date,
    paymentDate date,
    declarationDate date,
	ymd bigint
);
<?php

// create the connection to the latest sqlite database
App::uses('ConnectionManager', 'Model');
$dataSource = ConnectionManager::getDataSource('default');

$sqliteFolder = TMP.'sqlite';

$sqlite_db = array(
	'name' => 'snapshot',
	'datasource' => 'Database/Sqlite',
	'encoding' => 'utf8',
	'persistent' => false,
	'database' => $sqliteFolder. DS. $dataSource->config['database']. '_latest.sqlite',
	'snapshot' => false,
);

if(!is_dir($sqliteFolder))
	mkdir($sqliteFolder, 0777);
if(!is_file($sqliteFolder. DS. $dataSource->config['database']. '_latest.sqlite'))
{
	touch($sqliteFolder. DS. $dataSource->config['database']. '_latest.sqlite');
	chmod($sqliteFolder. DS. $dataSource->config['database']. '_latest.sqlite', 0777);
}
ConnectionManager::create('snapshot', $sqlite_db);
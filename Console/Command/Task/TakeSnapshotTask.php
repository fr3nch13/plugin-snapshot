<?php

App::uses('ConnectionManager', 'Model');
class TakeSnapshotTask extends AppShell
{
	public $uses = ['User'];
	
	public $tasks = ['Utilities.Out'];
	
	public $lastErrorMessage = null;
	
	public $Shell;
	
	public $Out;
	
	public $defaultDataSource = null;
	public $defaultDBconfig = null;
	public $defaultDBname = null;
	
	public $backupDataSource = null;
	public $backupDBconfig = null;
	public $backupDBname = null;
	
	public $sqliteFolder = false;
	
	public $ignoreTables = [
		'dblogs', 'cacher_queries', 
		'memcache_servers', 'memcache_server_logs',
		'proctimes', 'proctime_sql_stats', 'proctime_queries',
		'source_stat_logs', 'source_stats',
		'login_histories', 'usage_counts', 'usage_entities',
		'validation_errors',
	];
	
	public function execute(Shell $Shell)
	{
		$this->Shell = $Shell;
		$this->Out = $this->Shell->Tasks->load('Utilities.Out');
		$this->Out->calling_method_back = 2;
		
		$this->sqliteFolder = TMP.'sqlite';
		
		// make sure there is a sqlite folder in the cache
		if(!is_dir($this->sqliteFolder))
			mkdir($this->sqliteFolder);
			
		$this->setLastError();
		
		$this->info(__('Taking a snapshot of this portal\'s database.'));
		
		$this->loadDatasources();
		$this->emptyBackupDatabase();
		
		$this->info(__('Getting the tables of the DEFAULT database.'));
		$tables = $this->dbTables($this->defaultDataSource);
		
		$this->info(__('Found %s tables in the DEFAULT database.', count($tables)));
		
		foreach($tables as $table)
		{
			$this->copyTable($table);
		}
		$this->info(__('Completed copying the tables to the backup database.'));
		
		$this->copyBackupToSqlite();
		
		// this is at the end of this process.
		// i empty the backup db after the sqlite table is created so that it doesn't take up a bunch of HD space when backing up the databases.
		$this->emptyBackupDatabase();
	}
	
	public function loadDatasources()
	{
		$this->info(__('Getting the DEFAULT datasource.'));
		try {
			$this->defaultDataSource = ConnectionManager::getDataSource('default');
		} catch ( Exception $e) {
			$this->defaultDataSource = false;
			$this->error(__('Unable to load the DEFAULT datasource. Code: %s - Message: %s - Trace: %s', $e->getCode(), $e->getMessage(), $e->getTraceAsString() ));
			return false;
		}
		$this->defaultDBconfig = $this->defaultDataSource->config;
		$this->defaultDBname = $this->defaultDBconfig['database'];
		
		$this->info(__('Getting the BACKUP datasource.'));
		try {
			$this->backupDataSource = ConnectionManager::getDataSource('backup');
		} catch ( Exception $e) {
			$this->backupDataSource = false;
			$this->error(__('Unable to load the BACKUP datasource. Code: %s - Message: %s - Trace: %s', $e->getCode(), $e->getMessage(), $e->getTraceAsString() ));
			return false;
		}
		$this->backupDBconfig = $this->backupDataSource->config;
		$this->backupDBname = $this->backupDBconfig['database'];
	}
	
	public function emptyBackupDatabase()
	{
		$this->info(__('Making sure the BACKUP database is empty.'));
		
		if(!$tables = $this->dbTables($this->backupDataSource))
		{
			$this->info(__('The BACKUP database is empty.'));
		}
		else
		{
			$this->info(__('The BACKUP database is has tables, it needs to be emptied.'));
			
			foreach($tables as $table)
			{
				$this->info(__('Dropping table "%s" from the BACKUP Database.', $table));
				$result = $this->backupDataSource->query(__('DROP TABLE IF EXISTS `%s`;', $table));
				$this->info(__('DROP table "%s" - result: %s', $table, $result));
			}
		}
	}
	
	public function copyTable($table = false)
	{
		if(!$table)
		{
			$this->warning('No table specified');
			return false;
		}
		
		$start = time();
		
		$this->info(__('CREATING table: %s from %s to %s', $table, $this->defaultDBname, $this->backupDBname));
		$query = __('CREATE TABLE `%s`.`%s` LIKE `%s`.`%s`;', $this->backupDBname, $table, $this->defaultDBname, $table);
		$result = $this->backupDataSource->query($query);
		
		$this->info(__('COPYING table: %s from %s to %s', $table, $this->defaultDBname, $this->backupDBname));
		$query = __('INSERT INTO `%s`.`%s` SELECT * FROM `%s`.`%s`;', $this->backupDBname, $table, $this->defaultDBname, $table);
		$result = $this->backupDataSource->query($query);
		
		$this->info(__('Finished copying table "%s" in %s seconds.', $table, (time()-$start)));
	}
	
	public function copyBackupToSqlite()
	{
		$this->info(__('Begin copying the backup database to an sqlite database.'));
		$sqlite_folder = $this->sqliteFolder;
		
		$sqlite_file = $sqlite_folder. DS. $this->defaultDBname. '_'. date('Y-m-d').'.sqlite';
		$sqlite_file_tmp = $sqlite_folder. DS. str_replace('cakephp_', '', $this->defaultDBname). '.sqlite.tmp';
		
		if(file_exists($sqlite_file_tmp))
			unlink($sqlite_file_tmp);
		
		$out = [];
		$resultId = 0;
		$sequel_path = exec('which sequel', $out, $resultId);
		if($resultId)
		{
			$this->error(__("Unable to find the `sequel` command please install it with 'apt-get install ruby ruby-dev libsqlite3-dev libmysqlclient-dev; gem install sequel mysql2 sqlite3'.\nReturn Code: %s\nOutput: %s", $resultId, implode("\n", $out)));
		}
		
		$sequelExe = __('%s mysql2://%s:%s@%s/%s -C sqlite://%s 2>&1', 
			$sequel_path,
			$this->backupDBconfig['login'],
			$this->backupDBconfig['password'],
			$this->backupDBconfig['host'],
			$this->backupDBconfig['database'],
			$sqlite_file_tmp
		);
		
		$this->info(__('Using command: %s', $sequelExe));
		
		$out = [];
		$resultId = 0;
		exec($sequelExe, $out, $resultId);  // maybe switch and use http://php.net/manual/en/function.popen.php
		if($resultId)
		{
			$this->error(__("Unable to run sequel to export %s to %s.\nReturn Code: %s\nOutput: %s", $this->backupDBconfig['database'], $sqlite_file_tmp, $resultId, implode("\n", $out)));
		}
		
		
		$this->info(__('Results of the sequal process: %s', implode("\n", $out)));
		$out = [];
		
		$filesize = filesize($sqlite_file_tmp);
		$filesize = $filesize / 1024;
		$filesize = $filesize / 1024;
		$filesize = number_format($filesize, 2);
		$this->info(__('Successfully created temp sqlite database. File size: %sMB', $filesize));
		
		$this->info(__('Moving temp sqlite database from: %s - to: %s', $sqlite_file_tmp, $sqlite_file));
		
		if(file_exists($sqlite_file_tmp))
		{
			if(!rename($sqlite_file_tmp, $sqlite_file))
			{
				$this->error(__('Unable to copy temp sqlite file. Temp: %s - Permanent: %s', $sqlite_file_tmp, $sqlite_file));
			}
		}
		
		if(!chmod($sqlite_file, 0666))
		{
			$this->error(__('Unable to chmod the sqlite database: %s', $sqlite_file));
		}
		
		// create a symlink to the latest one.
		$symlink_db = $sqlite_folder. DS. $this->defaultDBname. '_latest.sqlite';
			
		$this->info(__('Creating symlink %s pointing to %s', $symlink_db, $sqlite_file));
		
		if(file_exists($symlink_db))
		{
			if(!unlink($symlink_db))
			{
				$this->error(__('Unable to delete symlink: %s', $symlink_db));
			}
		}
		
		if(!symlink($sqlite_file, $symlink_db))
		{
			$this->error(__('Unable to create the symlink: %s', $symlink_db));
		}
		
		$this->info(__('Completed copying the backup database to an sqlite database.'));
	}
	
	public function dbTables($dataSource = false, $ignoreTables = true)
	{
		// get the tables, and filter out the ones that we don't need to snapshot
		$dbTables = $dataSource->listSources();
		
		if($ignoreTables)
		{
			$ignoreTables = $this->ignoreTables;
		
			// see if we have app specific tables to ignore
			if(isset($dataSource->config['ignore_tables']) and is_array($dataSource->config['ignore_tables']) and $dataSource->config['ignore_tables'])
			{
				$ignoreTables = array_merge($ignoreTables, $dataSource->config['ignore_tables']);
			}
			
			// filter out any duplicates
			$ignoreTables = array_flip($ignoreTables);
		
			// filter out the ignore tables
			foreach($dbTables as $i => $dbTable)
			{
				if(preg_match('/\s+/', $dbTable))
					unset($dbTables[$i]);
				if(isset($ignoreTables[$dbTable]))
					unset($dbTables[$i]);
			}
		}
		
		return $dbTables;
	}
	
	private function info($msg = false)
	{
		
		return $this->Out->info($msg);
	}
	
	public function notice($msg = false)
	{
		return $this->Out->notice($msg);
	}
	
	public function warning($msg = false)
	{
		return $this->Out->warning($msg);
	}
	
	public function debug($msg = false)
	{
		return $this->Out->debug($msg);
	}
	
	public function error($msg = false, $sendEmail = true)
	{
		$this->Out->error($msg);
		$this->setLastError($msg);
		$this->emailError();
		throw new Exception(__('Error: %s', $msg));
	}
	
	public function setLastError($error = null)
	{
		$this->lastErrorMessage = $error;
	}
	
	public function getLastErrorMesssag()
	{
		return $this->lastErrorMessage;
	}
	
	private function emailError()
	{
		// get the admin users
		$emails = $this->User->adminEmails();
		
		if(!$emails)
		{
			$this->notice(__('No admin email addresses found.'));
			return false;
		}
		
		$body = __("An error occured while trying to snapshot the database.\n Error:\n%s", $this->getLastErrorMesssag());
		
		$Email = $this->Shell->Tasks->load('Utilities.Email');
		$Email->set('to', $emails);
		$Email->set('subject', __('Snapshot Error'));
		$Email->set('body', $body);
		$Email->execute();
	}
}
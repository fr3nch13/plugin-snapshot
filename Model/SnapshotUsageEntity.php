<?php
App::uses('AppModel', 'Model');
App::uses('UsageEntity', 'Usage.Model');
App::uses('Inflector', 'Utility');

class SnapshotUsageEntity extends UsageEntity 
{
	public $sqliteFolder = false;
	
	// tables to ignore when dealing with snapshots
	public $ignoreTables = array(
		'dblogs', 'cacher_queries', 
		'memcache_servers', 'memcache_server_logs',
		'proctimes', 'proctime_sql_stats', 'proctime_queries',
		'source_stat_logs', 'source_stats',
		'login_histories', 'usage_counts', 'usage_entities',
		'validation_errors',
	);
	
	public $dataSources = array();
	
	public function __construct($id = false, $table = null, $ds = null)
	{
		$this->sqliteFolder = TMP.'sqlite';
		
		// make sure there is a sqlite folder in the cache
		if(!is_dir($this->sqliteFolder))
			mkdir($this->sqliteFolder);
			
		return parent::__construct($id, $table, $ds);
	}
	
	public function afterFind($results = array(), $primary = false)
	{
		// fix the finder options
		foreach($results as $i => $result)
		{
			if(!isset($result[$this->alias]))
				continue;
			
			if(array_key_exists('finder_options', $result[$this->alias]))
			{
				$results[$i][$this->alias]['finder_options'] = json_decode($result[$this->alias]['finder_options']);
				if(!$results[$i][$this->alias]['finder_options'])
				{
					$results[$i][$this->alias]['finder_options'] = array();
				}
			}
		}
		
		// get the defined counts for this record of it's associated records based on a passed condition
		return parent::afterFind($results, $primary);
	}
	
/*
 * Created the snapshot of the current state of the database,
 * and exports it to an sqlite database with today's timestamp in the filename.
 */
	public function takeSnapshot()
	{
		$time_start = microtime(true);
		$this->shellOut(__('Taking a snapshot of the current database(s)'), 'snapshot');
		
		App::uses('ConnectionManager', 'Model');
		
		// make sure we have all the needed sources activated
		$sourceObjects = ConnectionManager::enumConnectionObjects();
		
		foreach($sourceObjects as $sourceName => $sourceSettings)
		{
			if(isset($sourceSettings['snapshot']) and !$sourceSettings['snapshot'])
				continue;
			ConnectionManager::getDataSource($sourceName);
		}
		
		$sourceList = ConnectionManager::sourceList();
		
		// location of the script that converts the sqldump file to an sqlite database
		$mysql2sqlite = ROOT. DS. 'Plugin'. DS. 'Snapshot'. DS. 'bin'. DS. 'mysql2sqlite.sh';
		$sqlite = trim(shell_exec('which sqlite3'));
		
		$sqlite_folder = $this->sqliteFolder;
		
		foreach($sourceList as $sourceName)
		{
			if($sourceName == 'snapshot')
				continue;
			if($sourceName == 'cacher')
				continue;
			
			$dataSource = $this->getSnapshotDataSource($sourceName);
			$config = $dataSource->config;
			
			$this->shellOut(__('Taking a snapshot of source: %s - database: %s', $sourceName, $config['database']), 'snapshot');
			
			$sqlite_file = $sqlite_folder. DS. $config['database']. '_'. date('Y-m-d').'.sqlite';
			$sqldump_file = $sqlite_folder. DS. $config['database'].'.sqlite_dump';
			$sqlite_file_tmp = $sqlite_file.'.tmp';
			
			if(file_exists($sqlite_file_tmp))
				unlink($sqlite_file_tmp);
	
			// get the list of valid database tables to snapshot
			$dbTables = $this->dbTables($dataSource);
			
			$mysql2sqliteExe = __('%s -u%s -p%s %s %s > %s', 
				$mysql2sqlite,
				$config['login'],
				$config['password'],
				$config['database'],
				implode(' ', $dbTables),
				$sqldump_file
			);
			
			$this->shellOut(__('Dumping the MySQL database to: %s - Tables: %s', $sqldump_file, implode(' ', $dbTables)), 'snapshot');
			shell_exec($mysql2sqliteExe);
			
			$filesize = filesize($sqldump_file);
			$filesize = $filesize / 1024;
			$filesize = $filesize / 1024;
			$filesize = number_format($filesize, 2);
			$this->shellOut(__('Dump complete dump file size: %sMB', $filesize), 'snapshot');
			
			$mysql2sqliteExe = __('%s %s < %s', 
				$sqlite,
				$sqlite_file_tmp,
				$sqldump_file
			);
			
			$this->shellOut(__('Creating Sqlite database from mysql dump. command: %s', $mysql2sqliteExe), 'snapshot');
			
			// results 
			$results = false;
			if($results = trim(shell_exec($mysql2sqliteExe)))
			{
			
				$filesize = filesize($sqlite_file_tmp);
				$filesize = $filesize / 1024;
				$filesize = $filesize / 1024;
				$filesize = number_format($filesize, 2);
				$this->shellOut(__('Successfully created temp sqlite database. File size: %sMB', $filesize), 'snapshot');
				
				$this->shellOut(__('Moving temp sqlite database from: %s - to: %s', $sqlite_file_tmp, $sqlite_file), 'snapshot');
				
				if(file_exists($sqlite_file_tmp))
					rename($sqlite_file_tmp, $sqlite_file);
				
				chmod($sqlite_file, 0666);
				
				// create a symlink to the latest one.
				$symlink_db = $this->sqliteFolder. DS. $config['database']. '_latest.sqlite';
				
				if(file_exists($symlink_db))
					unlink($symlink_db);
				
				symlink($sqlite_file, $symlink_db);
				$this->shellOut(__('Snapshot taken of source: %s - database: %s', $sourceName, $config['database']), 'snapshot');
			}
			else
			{
				$this->shellOut(__('Unable to import the mysqldump to sqlite. Dump File: %s Sqlite File: %s', $sqldump_file, $sqlite_file_tmp), 'snapshot', 'error');
				continue;
			}
			
		}
		
		
		$time_end = microtime(true);
		$time = $time_end - $time_start;
		$time = number_format($time, 2);
		$this->shellOut(__('Completed snapshot of the current database(s) - took %s seconds', $time), 'snapshot');

		return $results;
	}
	
/*
 * Makes sure that the static/hard-code snapshot entity (defined in $Model->actsAs)
 * has a record in the UsageEntity model/database table.
 */
	public function checkEntities($modelFilter = [])
	{
		$this->shellOut(__('Checking to make sure all defined Snapshot Usage entities are being tracked in the database.'), 'snapshot');
		
		// allowed, and configured models;
		if($modelFilter)
		{
			if(!is_array($modelFilter))
				$modelFilter = explode(',', $modelFilter);
		}
		$dbModels = $this->dbModels($modelFilter);
		
		$this->shellOut(__('Found %s Models that are configured to use Snapshot.', count($dbModels)), 'snapshot');
		
		$usageEntities = array();
		
		// make sure each configured stat for each model has a UsageEntity associated with it
		foreach($dbModels as $i => $dbModel)
		{
			list($dbPlugin, $dbModel) = pluginSplit($dbModel, true);
			
			$actsAs = $this->{$dbModel}->actsAs['Snapshot.Stat'];
			if(!isset($actsAs['entities']))
				continue;
			if(!is_array($actsAs['entities']))
				continue;
			$entities = $actsAs['entities'];
			
			$this->shellOut(__('Found %s Defined Entities for the Model: %s', count($entities), $dbModel), 'snapshot');
			
			foreach($entities as $entityKey => $entityOptions)
			{
				$entityKey = $this->entityKey($entityKey, $dbModel, $dbPlugin);
				
				// make sure there's an UsageEntity record for this.
				$usageEntities[$entityKey] = $this->checkAdd($entityKey, 'snapshot', $data = array(
					'model' => $dbPlugin.$dbModel,
					'finder_options' => $entityOptions,
					'type' => 'snapshot',
				));
			}
		}
		
		$this->shellOut(__('All Defined Entities have been checked.'), 'snapshot');
		
		return $usageEntities;
	}
	
/*
 * Update the Usage Entities in the group 'snapshot'
 */ 
	public function updateEntities($checkNew = false)
	{
		$time_start = microtime(true);
		$this->shellOut(__('Updating all entities.'), 'snapshot');
		
		if($checkNew)
		{
			$newUsageEntities = $this->find('list', array(
				'conditions' => array(
					$this->alias.'.group' => 'snapshot',
					$this->alias.'.compiled' => false,
					$this->alias.'.dynamic' => false,
				),
				'fields' => array(
					$this->alias.'.id',
					$this->alias.'.key'
				),
			));
			
			$this->shellOut(__('Found %s New Entities that need to have their history compiled.', count($newUsageEntities)), 'snapshot');
			
			$dataSource = $this->getSnapshotDataSource('snapshot');
			$config = $dataSource->config;
			
			// compile the stats including the historical stats for the newly discovered entities
			if($newUsageEntities)
			{
				$updateCnt = 0;
				if($this->compileEntitiesCounts($newUsageEntities, true))
				{
					foreach($newUsageEntities as $entityId => $entityKey)
					{
						$this->id = $entityId;
						$this->data = array('compiled' => true, 'last_updated' => date('Y-m-d H:i:s'));
						if($this->save($this->data))
						{
							$updateCnt++;
						}
					}
				}
				$this->shellOut(__('Compiled %s New Entities.', $updateCnt), 'snapshot');
			}
		}
		// only existing ones
		else
		{
			$usageEntities = $this->find('list', array(
				'conditions' => array(
					$this->alias.'.group' => 'snapshot',
					$this->alias.'.compiled' => true,
					$this->alias.'.dynamic' => false,
				),
				'fields' => array(
					$this->alias.'.id',
					$this->alias.'.key'
				),
			));
			$this->shellOut(__('Found %s Existing Entities that need to be updated.', count($usageEntities)), 'snapshot');
			
			if($usageEntities)
			{
				$updateCnt = 0;
				if($this->compileEntitiesCounts($usageEntities, false))
				{
					foreach($usageEntities as $entityId => $entityKey)
					{
						$this->id = $entityId;
						$this->data = array('last_updated' => date('Y-m-d H:i:s'));
						if($this->save($this->data))
						{
							$updateCnt++;
						}
					}
				}
				$this->shellOut(__('Updated %s Existings Entities.', $updateCnt), 'snapshot');
			}
		}
		$time_end = microtime(true);
		$time = $time_end - $time_start;
		$this->shellOut(__('Completed updating of New/Exising Entities - took %s seconds', $time), 'snapshot');
	}
	
/*
 * Updates the usage counts from the latest Snapshot, 
 * for from all of the Snapshots if backCompile is true
 */
	public function compileEntitiesCounts($entityKeys = array(), $backCompile = false)
	{
		if(!$entityKeys)
			return false;
		
		if(!is_array($entityKeys) and (is_int($entityKeys) or is_string($entityKeys)))
		{
			$entityKeys = array($entityKeys => $entityKeys);
		}
		
		$modelNames = array();
		foreach($entityKeys as $entityId => $entityKey)
		{
			$parts = explode('.', $entityKey);
			
			$inPlugin = false;
			$pluginName = $modelName = $snapshotKey = false;
			if(count($parts) == 3)
			{
				$inPlugin = true;
				list($pluginName, $modelName, $snapshotKey) = $parts;
				$pluginName = Inflector::camelize($pluginName);
				$modelName = Inflector::camelize($modelName);
				$modelNames[$pluginName.'.'.$modelName] = $pluginName.'.'.$modelName;
			}
			elseif(count($parts) == 2)
			{
				list($modelName, $snapshotKey) = $parts;
				$modelName = Inflector::camelize($modelName);
				$modelNames[$modelName] = $modelName;
			}
		}
		
		$modelNames = $this->dbModels($modelNames);
		
		// track original schema names
		$origSchemaNames = array();
		$origDataSources = array();
		foreach($modelNames as $modelName)
		{
			$modelNameOrig = $modelName;
			list($pluginName, $modelName) = pluginSplit($modelName, true);
			$origSchemaNames[$modelNameOrig] = $this->{$modelName}->schemaName;
			$origDataSources[$modelNameOrig] = $this->{$modelName}->useDbConfig;
		}
		
		// track/update counts
		$entities = array();
		$snapshots = array();
		
		// compile the counts from the previous snapshots
		if($backCompile)
		{
			$snapshots = $this->snapshotsList();
		}
		// otherwise use the latest as defined in the snaphot data source (Config/bootstrap.php)
		else
		{
			if($snapshotPathLatest = $this->snapshotsLatest())
			{
				$snapshots[basename($snapshotPathLatest)] = $snapshotPathLatest;
			}
		}
		
		foreach($snapshots as $snapshotFile => $snapshotPath)
		{	
			// try to figure out the timestamp
			$matches = array();
			if(!preg_match('/(\d+)\-(\d+)\-(\d+)/', $snapshotFile, $matches))
				continue;
			
			if(count($matches) != 4)
				continue;
			
			$timeStamp_hour = $matches[1].$matches[2].$matches[3].date('H');
			$timeStamp_day = $matches[1].$matches[2].$matches[3];
			$timeStamp_week = $matches[1].date('W', strtotime($matches[0]));
			$timeStamp_month = $matches[1].$matches[2];
			$timeStamp_year = $matches[1];
			
			// now go through the configured entities and compile their counts
			foreach($modelNames as $modelName)
			{
				$modelNameOrig = $modelName;
				list($pluginName, $modelName) = pluginSplit($modelName, true);
				$configuredEntities = $this->{$modelName}->actsAs['Snapshot.Stat'];
				$this->shellOut(__('Compiling for Model: %s', $pluginName.$modelName), 'snapshot');
				
				if(!$dataSource = $this->getSnapshotDataSource('snapshot_'. basename($snapshotPath)))
				{
					$this->shellOut(__('Issue with dataSource (3) for Model: %s database: ', $modelName, basename($snapshotPath)), 'snapshot', 'warning');
					continue;
				}
				
				$snapshotOtherPath = false;
				if($this->{$modelName}->schemaName and !preg_match("/".$this->{$modelName}->schemaName."/", $dataSource->config['database']))
				{
					$snapshotOtherPath = $this->sqliteFolder. DS. $this->{$modelName}->schemaName. '_'. $matches[0].'.sqlite';
					if(!file_exists($snapshotOtherPath))
					{
						$this->shellOut(__('Unknown File for snapshot database - Model: %s database: %s', $modelName, basename($snapshotOtherPath)), 'snapshot', 'warning');
						continue;
					}
					
					$this->createSnapshotDataSource($snapshotOtherPath);
					if(!$dataSource = $this->getSnapshotDataSource('snapshot_'. basename($snapshotOtherPath)))
					{
						$this->shellOut(__('Issue with dataSource (2) for Model: %s database: %s', $modelName, basename($snapshotOtherPath)), 'snapshot', 'warning');
						continue;
					}
				}
				
				// make sure the table for this model exists in the database
				$tables = $dataSource->listSources();
				if(!in_array($this->{$modelName}->useTable, $tables)) 
				{
					$this->shellOut(__('Table %s doesn\'t exist for Model: %s in database %s - moving on.', $this->{$modelName}->useTable, $pluginName.$modelName, $dataSource->config['name']), 'snapshot', 'warning');
					continue;
				}
				
				// have the model use the switched database
				$dataSourceOrig = (isset($this->{$modelName}->getDataSource()->config['name'])?$this->{$modelName}->getDataSource()->config['name']:'default');
				$schemaNameOrig = $this->{$modelName}->schemaName;
				$this->{$modelName} = new $modelName(false, null, $dataSource->config['name']);
				$this->{$modelName}->cacheQueries = false;
				$this->{$modelName}->schemaName = 'main';
				
				if(isset($configuredEntities['entities']))
				{
					foreach($configuredEntities['entities'] as $entityKey => $entityOptions)
					{
						$entityKey = $this->entityKey($entityKey, $modelName, $pluginName);
						$this->shellOut(__('Compiling count(s) for Entity: %s from database: %s', $entityKey, basename($this->{$modelName}->getDataSource()->config['database'])), 'snapshot');
						
						$entityOptions = $this->entityOptions($modelName, $entityKey, $entityOptions, $matches[0]);
						
						$count = 0;
						// find a count of all of the ones created just today
						if(preg_match('/\.(created|modified)$/', $entityKey))
						{
							$_entityOptions = $entityOptions;
							$_entityOptions['recursive'] = -1;
							$objects = array();
							
							if(preg_match('/\.created$/', $entityKey))
							{
								$_entityOptions['fields'] = array($this->{$modelName}->alias.'.id', $this->{$modelName}->alias.'.created');
							}
							elseif(preg_match('/\.modified$/', $entityKey))
							{
								$_entityOptions['fields'] = array($this->{$modelName}->alias.'.id', $this->{$modelName}->alias.'.modified');
							}
							
							$objects = $this->{$modelName}->find('list', $_entityOptions);
							
							$this->shellOut(__('1 - Found %s counts for Entity: %s from database: %s', count($objects), $entityKey, basename($this->{$modelName}->getDataSource()->config['database'])), 'snapshot');
							
							if(!isset($this->timePeriods))
							{
								if(!isset($this->UsageCount))
								{
									// reload the UsageCount
									App::uses('UsageCount', 'Usage.Model');
									$this->UsageCount = new UsageCount();
								}
								$this->timePeriods = $this->UsageCount->time_periods;
								
								if(isset($this->timePeriods['minute']))
									unset($this->timePeriods['minute']);
							}
							
							$this_entities = array();
							foreach($objects as $objectId => $created)
							{
								foreach($this->timePeriods as $timePeriod => $timeFormat)
								{
									$this_created = date($timeFormat, strtotime($created));
									
									if(!isset($this_entities[$entityKey.'-'.$timePeriod.'-'.$this_created])) 
										$this_entities[$entityKey.'-'.$timePeriod.'-'.$this_created] = 0; 
									
									$this_entities[$entityKey.'-'.$timePeriod.'-'.$this_created]++;
								}
							}
							
							foreach($this_entities as $entity_id => $entity_count)
							{
								if(!isset($entities[$entity_id])) 
									$entities[$entity_id] = 0; 
								
								if($entities[$entity_id] < $entity_count)
									$entities[$entity_id] = $entity_count;
							}
						}
						else
						{
							$count = $this->{$modelName}->find('count', $entityOptions);
							$this->shellOut(__('2 - Found %s counts for Entity: %s from database: %s', $count, $entityKey, basename($this->{$modelName}->getDataSource()->config['database'])), 'snapshot');
						
							$entities[$entityKey.'-hour-'.$timeStamp_hour] = $count;
							$entities[$entityKey.'-day-'.$timeStamp_day] = $count;
							if(!isset($entities[$entityKey.'-week-'.$timeStamp_week])) $entities[$entityKey.'-week-'.$timeStamp_week] = 0;
							if($entities[$entityKey.'-week-'.$timeStamp_week] < $count)
								$entities[$entityKey.'-week-'.$timeStamp_week] = $count;
							if(!isset($entities[$entityKey.'-month-'.$timeStamp_month])) $entities[$entityKey.'-month-'.$timeStamp_month] = 0;
							if($entities[$entityKey.'-month-'.$timeStamp_month] < $count)
								$entities[$entityKey.'-month-'.$timeStamp_month] = $count;
							if(!isset($entities[$entityKey.'-year-'.$timeStamp_year])) $entities[$entityKey.'-year-'.$timeStamp_year] = 0;
							if($entities[$entityKey.'-year-'.$timeStamp_year] < $count)
								$entities[$entityKey.'-year-'.$timeStamp_year] = $count;
						}
					}
				}
				
				// now that we're done, restore the original datasource and schema name.
				// this way, if we come across this model again, it will be treated like the first time
				if(isset($origDataSources[$modelNameOrig]))
				{
					$this->{$modelName}->setDataSource($origDataSources[$modelNameOrig]);
				}
				
				if(isset($origSchemaNames[$modelNameOrig]))
				{
					$this->{$modelName}->schemaName = $origSchemaNames[$modelNameOrig];
				}
			}	
		}
		
		if($entities)
		{
			ksort($entities);
			foreach($entities as $entityKey => $entityCount)
			{
				list($entityKey, $timePeriod, $timeStamp) = explode('-', $entityKey);
				
				$modelName = explode('.', $entityKey);
				array_pop($modelName);
				$modelName = implode('.', $modelName);
				
				list($pluginName, $modelName) = pluginSplit($modelName, true);
				
				$this->updateCount($entityKey, 'snapshot', $entityCount, $pluginName.$modelName, $timePeriod, $timeStamp, true);
			}
		}
		return count($entities);
	}
	
	public function updateStats($backCompile = false, $finder = false, $modelFilters = [])
	{
		$this->needsBackCompile = false;
		
		// allowed, and configured models;
		if($modelFilters)
		{
			if(!is_array($modelFilters))
				$modelFilters = explode(',', $modelFilters);
				$modelFilters = array_map('trim', $modelFilters);
		}
		$modelNames = $this->dbModels($modelFilters);
		
		foreach($modelNames as $i => $modelName)
		{
			list($dbPlugin, $modelName) = pluginSplit($modelName, true);
			
			if(!isset($this->{$modelName}) or !$this->{$modelName})
			{
				unset($modelNames[$i]);
				continue;
			}
			
			if(!method_exists($this->{$modelName}, 'snapshotStats'))
			{
				unset($modelNames[$i]);
				continue;
			}
		}
		
		// compile the counts from the previous snapshots
		$snapshots = array();
		
		// compile the counts from the previous snapshots
		if($backCompile)
		{
			$snapshots = $this->snapshotsList(false, true, $finder);
		}
		elseif($finder)
		{
			$snapshots = $this->snapshotsList(false, true, $finder);
		}
		// otherwise use the latest as defined in the snaphot data source (Config/bootstrap.php)
		else
		{
			if($snapshotPathLatest = $this->snapshotsLatest())
			{
				$snapshots[basename($snapshotPathLatest)] = $snapshotPathLatest;
			}
		}
		
		$entities = array();
		foreach($snapshots as $snapshotFile => $snapshotPath)
		{
			foreach($modelNames as $modelName)
			{
				$thisEntities = $this->updateStatsInstance($modelName, $snapshotFile, $snapshotPath, $backCompile);
			}
		}
		
		if($this->needsBackCompile)
		{
			$this->updateStats(true);
		}
	}
	
	public function updateStatsInstance($modelName = false, $snapshotFile = false, $snapshotPath = false, $backCompile = false)
	{
		// use this snapshot as our current database
		if(!$this->createSnapshotDataSource($snapshotPath))
			return array();
		
		if(!$dataSource = $this->getSnapshotDataSource('snapshot_'. basename($snapshotPath)))
		{
			$this->shellOut(__('Issue with dataSource (3) for Model: %s database: ', $modelName, basename($snapshotPath)), 'snapshot', 'warning');
			return array();
		}
		
		// try to figure out the timestamp
		$matches = array();
		if(!preg_match('/(\d+)\-(\d+)\-(\d+)/', $snapshotFile, $matches))
			return array();
		
		if(count($matches) != 4)
			return array();
		
		$timeStamp_hour = $matches[1].$matches[2].$matches[3].date('H');
		$timeStamp_day = $matches[1].$matches[2].$matches[3];
		$timeStamp_week = $matches[1].date('W', strtotime($matches[0]));
		$timeStamp_month = $matches[1].$matches[2];
		$timeStamp_year = $matches[1];
		
		list($pluginName, $modelName) = pluginSplit($modelName, true);
		$configuredEntities = $this->{$modelName}->actsAs['Snapshot.Stat'];
		$this->shellOut(__('Compiling for Model: %s - database: %s', $pluginName.$modelName, basename($snapshotPath)), 'snapshot');
		
		$snapshotOtherPath = false;
		if($this->{$modelName}->schemaName and !preg_match("/".$this->{$modelName}->schemaName."/", $dataSource->config['database']))
		{
			$snapshotOtherPath = $this->sqliteFolder. DS. $this->{$modelName}->schemaName. '_'. $matches[0].'.sqlite';
			
			if(!file_exists($snapshotOtherPath))
			{
				$this->shellOut(__('Unknown File for snapshot database - Model: %s database: %s', $modelName, basename($snapshotOtherPath)), 'snapshot', 'warning');
				return array();
			}
			
			$this->createSnapshotDataSource($snapshotOtherPath);
			if(!$dataSource = $this->getSnapshotDataSource('snapshot_'. basename($snapshotOtherPath)))
			{
				$this->shellOut(__('Issue with dataSource (2) for Model: %s database: %s', $modelName, basename($snapshotOtherPath)), 'snapshot', 'warning');
				return array();
			}
		}
		
		if(!isset($this->timePeriods))
		{
			if(!isset($this->UsageCount))
			{
				// reload the UsageCount
				App::uses('UsageCount', 'Usage.Model');
				$this->UsageCount = new UsageCount();
			}
			$this->timePeriods = $this->UsageCount->time_periods;
			
			if(isset($this->timePeriods['minute']))
				unset($this->timePeriods['minute']);
		}
		
		$timestamps = array($matches[0]);
		foreach($this->timePeriods as $timePeriod => $timeFormat)
		{
			$timestamp = date($timeFormat, strtotime($matches[0]));
			$timestamps[$timePeriod] = $timestamp;
		}
		
		// have the model use the switched database
		$dataSourceOrig = (isset($this->{$modelName}->getDataSource()->config['name'])?$this->{$modelName}->getDataSource()->config['name']:'default');
		$schemaNameOrig = $this->{$modelName}->schemaName;
		
		// reload the model
		$this->{$modelName} = new $modelName(false, null, $dataSource->config['name']);
		
		$this->{$modelName}->cacheQueries = false;
		$this->{$modelName}->schemaName = 'main';
		$this->{$modelName}->snapshotStats = array(
			'timestamps' => $timestamps,
			'timePeriods' => $this->timePeriods,
			'entityPrefix' => $this->entityKey(false, $modelName, $pluginName),
			'modelName' => $modelName,
			'pluginName' => $pluginName,
			'SnapshotUsageEntity' => &$this,
			'snapshotFile' => $snapshotFile,
			'snapshotPath' => $snapshotPath,
			'backCompile' => $backCompile,
			
		);
		
		$this->needsBackCompile = false;
		
		$thisEntities = $this->{$modelName}->snapshotStats();
		if(isset($this->{$modelName}->needsBackCompile))
		{
			$this->needsBackCompile = $this->{$modelName}->needsBackCompile;
		}
		
		$this->{$modelName}->setDataSource($dataSourceOrig);
		$this->{$modelName}->cacheQueries = false;
		$this->{$modelName}->schemaName = $schemaNameOrig;
		
		return $thisEntities;
	}
	
	public function entityOptions($modelName = false, $entityKey = false, $entityOptions = array(), $timestamp = false)
	{
		if(!is_array($entityOptions))
			$entityOptions = array($entityOptions);
			
		if(!$modelName)
			return $entityOptions;
		
		if(!$entityKey)
			return $entityOptions;
		
		if(!$timestamp)
			return $entityOptions;
		
		if(!isset($entityOptions['conditions']))
			$entityOptions['conditions'] = array();
		
		return $entityOptions;
	}
	
	function snapshotsList($includeLatest = false, $createSnapshotDataSource = true, $finder = false)
	{
		App::uses('Folder', 'Utility');
		App::uses('File', 'Utility');
		
		$dir = new Folder($this->sqliteFolder);
		
		$dataSource = $this->getSnapshotDataSource('default');
		
		if($finder)
			$finder = str_replace('latest.sqlite', $finder.'.sqlite', $dataSource->config['database']. '_latest.sqlite');
		else
			$finder = str_replace('latest.sqlite', '\d+\-\d+\-\d+\.sqlite', $dataSource->config['database']. '_latest.sqlite');
		
		$files = $dir->find($finder);
		$files = array_flip($files);
		krsort($files);
		
		$latestKey = false;
		
		foreach($files as $file => $path)
		{
			$path = $dir->pwd() . DS . $file;
			
			// ignore the 'latest'
			if(is_link($path))
			{
				if(!$includeLatest)
				{
					unset($files[$file]);
					continue;
				}
				$path = readlink($path);
			}
			
			if($createSnapshotDataSource)
				$this->createSnapshotDataSource($path);
			
			$files[$file] = $path;
		}
		
		return $files;
	}
	
	function snapshotsLatest()
	{
		if($files = $this->snapshotsList(true))
		{
			return array_shift($files);
		}
		return false;
	}
	
	public function getSnapshotDataSource($name = false)
	{
		if(strpos($name, '/') !== false)
		{
			$name = basename($name);
		}
		
		if(!$name)
			return false;
		if(!isset($this->dataSources[$name]))
		{
			$dataSource = ConnectionManager::getDataSource($name);
			$this->dataSources[$name] = $dataSource;
		}
		
		return $this->dataSources[$name];
	}
	
	public function createSnapshotDataSource($database = false, $dataSourceName = 'snapshot')
	{
		if(strpos($database, '/') === false and $dataSourceName == 'snapshot')
		{
			$database = $this->sqliteFolder. DS. $database;
		}
		
		App::uses('ConnectionManager', 'Model');
		$dataSource = $this->getSnapshotDataSource($dataSourceName);
		
		$dataSource->config;
		
		if($database)
			$dataSource->config['database'] = $database;
		
		$nds = $dataSourceName.'_'.basename($dataSource->config['database']);
		
		if(isset($this->dataSources[$nds]))
			return $this->dataSources[$nds];
		
		$dataSource->setConfig(array('name' => $nds, 'database' => $dataSource->config['database'], 'persistent' => false));
		if($ds = ConnectionManager::create($nds, $dataSource->config))
		{
			$this->dataSources[$nds] = $ds;
			return $ds;
		}
		return false;
	}
	
/*
 * Creates a reliable Usage Entity key
 */
	public function entityKey($key = false, $modelName = false, $pluginName = false)
	{
		$parts = array();
		$key = trim($key);
		$key = trim($key, '.');
		
		if($pluginName)
		{
			$pluginName = trim($pluginName);
			$pluginName = trim($pluginName, '.');
			$parts[] = $pluginName;
		}
		
		if($modelName)
		{
			$modelName = trim($modelName);
			$modelName = trim($modelName, '.');
			$parts[] = $modelName;
		}
		$parts[] = $key;
		return implode('.', $parts);
	}
	
/*
 * returns a list of Models that are configured to use the Snapshot Behavior
 * and have a valid table ( see this::dbTables(); )
 */
	public function dbModels($modelNames = false, $pluginModelNames = false)
	{
		// get a list of all of the valid tables in the database
		$dbTables = $this->dbTables();
		$dbTables = array_flip($dbTables);
		
		if($modelNames !== false and is_array($modelNames) and !empty($modelNames))
		{
			$pluginModelNames = array();
			
			foreach($modelNames as $i => $modelName)
			{
				$pluginName = false;
				list($pluginName, $modelName) = pluginSplit($modelName);
				
				if($pluginName)
				{
					if(!isset($pluginModelNames[$pluginName]))
						$pluginModelNames[$pluginName] = array();
					
					$pluginModelNames[$pluginName][$modelName] = $modelName;
					unset($modelNames[$i]);
				}
			}
		}
		
		// find all of the core models
		$allModelNames = App::objects('Model');
		
		// if we have a defined set of modelNames, only use those
		if($modelNames !== false and is_array($modelNames) and !empty($modelNames))
		{
			$allModelNames = array_intersect($allModelNames, $modelNames);
		}
		
		// filter 
		foreach($allModelNames as $i => $modelName)
		{
			if(!isset($this->{$modelName}))
			{
				App::import('Model', $modelName);
				$this->{$modelName} = new $modelName();
			}
			$modelTable = $this->{$modelName}->useTable;
			
			// if it's table isn't in the known allowed database tables
			if(!isset($dbTables[$modelTable]))
			{
				unset($this->{$modelName});
				unset($allModelNames[$i]);
				continue;
			}
			
			// make sure this model is configured to work with the Snapshot Behavior
			if(!isset($this->{$modelName}->actsAs['Snapshot.Stat']))
			{
				unset($this->{$modelName});
				unset($allModelNames[$i]);
				continue;
			}
		}
		
		// get a list of the plugins
		$allPluginNames = App::objects('plugin');
		
		// if we have a defined set of modelNames, only use those
		if($pluginModelNames !== false and is_array($pluginModelNames))
		{
			$allPluginNames = array_intersect($allPluginNames, array_keys($pluginModelNames));
		}
		
		// get a list of all of the models in each loaded plugin
		foreach($allPluginNames as $pluginName)
		{
			if(!CakePlugin::loaded($pluginName))
				continue;
			
			$allPluginModelNames = App::objects($pluginName.'.Model');
			
			if(isset($pluginModelNames[$pluginName]) and is_array($pluginModelNames[$pluginName]))
			{
				$allPluginModelNames = array_intersect($allPluginModelNames, $pluginModelNames[$pluginName]);
			}
			
			// filter the plugin models just like the core ones
			foreach($allPluginModelNames as $pluginModelName)
			{
				if(!isset($this->{$pluginModelName}))
				{
					App::import('Model', $pluginName.'.'.$pluginModelName);
					$this->{$pluginModelName} = new $pluginModelName();
				}
				$modelTable = (isset($this->{$pluginModelName}->useTable)?$this->{$pluginModelName}->useTable:false);
				
				// if it's table isn't in the known allowed database tables
				if($modelTable and !isset($dbTables[$modelTable]))
				{
					unset($this->{$pluginModelName});
					unset($allModelNames[$i]);
					continue;
				};
				
				// make sure this model is configured to work with the Snapshot Behavior
				if(!isset($this->{$pluginModelName}->actsAs['Snapshot.Stat']))
				{
					unset($this->{$pluginModelName});
					unset($allModelNames[$i]);
					continue;
				}
				$allModelNames[] = $pluginName.'.'.$pluginModelName;
			}
		}
		
		foreach($allModelNames as $modelName)
		{
			if(!isset($this->{$modelName}))
			{
				list($dbPlugin, $modelName) = pluginSplit($modelName, true);
				App::uses($pluginModelName, $dbPlugin.'Model');
				$this->{$modelName} = new $modelName();
			}
		}
		return $allModelNames;
	}
	
/* 
 * Compiles a list of tables that are allowed to be backed up
 */
	public function dbTables($dataSource = false)
	{
		if(!$dataSource)
		{
			App::uses('ConnectionManager', 'Model');
			$dataSource = ConnectionManager::getDataSource('default');
		}
		
		// get the tables, and filter out the ones that we don't need to snapshot
		$dbTables = $dataSource->listSources();
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
		
		return $dbTables;
	}
}
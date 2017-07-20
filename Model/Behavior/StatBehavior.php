<?php
/* 
 * Used to hold common functions across all apps
 */

App::uses('Hash', 'Core');
App::uses('CakeEmail', 'Network/Email');
App::uses('CookieComponent', 'Controller/Component');
class StatBehavior extends ModelBehavior 
{
	public $settings = array();
	
	private $_defaults = array();
	
	public $UsageEntity = false;
	
	public $snapshotKeysCache = array();
	
	public function setup(Model $Model, $config = array()) 
	{
		// merge the default settings with the model specific settings
		$this->settings[$Model->alias] = array_merge($this->_defaults, $config);
	}
	
	public function Snapshot_availableKeys(Model $Model)
	{
		$this->loadUsageEntity();
		
		if(!isset($this->snapshotKeysCache[$Model->alias]))
		{
			$modelAlias = Inflector::underscore($Model->alias);
			$this->snapshotKeysCache[$Model->alias] = $this->UsageEntity->find('list', array(
				'conditions' => array(
					'UsageEntity.group' => 'snapshot',
					'UsageEntity.key LIKE' => $modelAlias.'.%',
				),
				'fields' => array('UsageEntity.id', 'UsageEntity.key'),
			));
		}
		
		return $this->snapshotKeysCache[$Model->alias];
	}
	
	public function Snapshot_keys(Model $Model, $snapshotKeyRegex = false)
	{
		$snapshotKeys = $this->Snapshot_availableKeys($Model);
		
		if(!$snapshotKeyRegex)
			return $snapshotKeys;
		
		// filter the keys
		foreach($snapshotKeys as $id => $key)
		{
			if(!preg_match($snapshotKeyRegex, $key))
				unset($snapshotKeys[$id]);
		}
		
		return $snapshotKeys;
	}
	
	public function Snapshot_dashboardStats(Model $Model, $snapshotKeyRegex = false, $start = false, $end = false, $scope = false)
	{
		$this->loadUsageEntity();
		$snapshotKeys = $this->Snapshot_keys($Model, $snapshotKeyRegex);
		
		if(empty($snapshotKeys))
		{
			$Model->modelError = __('No snapshot keys given.');
			return false;
		}
		
		if(!$start)
			$start = date('Y-m-d');
		
		if(!$end)
			$end = date('Y-m-d', strtotime('-30 days', strtotime($start)));
		
		$countMatrix = $this->UsageEntity->UsageCount->countMatrixRange(false, 'day', $start, $end);
		
		$assocModels = array();
		foreach($snapshotKeys as $snapshotEntity_id => $snapshotEntity_key)
		{
			$key_parts = explode('.', $snapshotEntity_key); // get rid of this $Model's name
			array_shift($key_parts);
			foreach($key_parts as $key_part)
			{
				list($assocModel_alias, $assocModel_id) = explode('-', $key_part);
				if(!isset($assocModels[$assocModel_alias]))
					$assocModels[$assocModel_alias] = array();
				$assocModels[$assocModel_alias][$assocModel_id] = $assocModel_id;
			}
		}
		
		foreach($assocModels as $modelAlias => $modelIds)
		{
			$modelAliasCamelized = Inflector::camelize($modelAlias);
			if(isset($Model->{$modelAliasCamelized}))
			{
				$assocModels[$modelAlias] = $Model->{$modelAliasCamelized}->find('list', array(
					'conditions' => array(
						$modelAliasCamelized.'.'.$Model->{$modelAliasCamelized}->primaryKey => $modelIds,
					),
				));
			}
			else
			{
				unset($assocModels[$modelAlias]);
			}
		}
		
		$assocModels = Hash::flatten($assocModels, '-');
		
		$snapshotKeysNamed = array();
		foreach($snapshotKeys as $snapshotEntity_id => $snapshotEntity_key)
		{
			$key_parts = explode('.', $snapshotEntity_key); // get rid of this $Model's name
			$this_model = array_shift($key_parts);
			$key_parts_named = array();
			foreach($key_parts as $key_part)
			{
				$key_parts_named[$key_part] = '';
				if(isset($assocModels[$key_part]))
					$key_parts_named[$key_part] = $assocModels[$key_part];
				else
					unset($key_parts_named[$key_part]);
			}
			
			$snapshotKeysNamed[$snapshotEntity_key] = implode(' - ', $key_parts_named);
		}
		
		$out = array(
			'legend' => array(
				'day' => __('Day'),
			),
			'data' => array(),
		);
		
		foreach($snapshotKeysNamed as $snapshotKeysNamed_key => $snapshotKeysNamed_name)
		{
			$out['legend'][$snapshotKeysNamed_key] = $snapshotKeysNamed_name;
		}
		
		$i = 0;
		foreach($countMatrix as $timeStamp => $zero)
		{
			$out['data'][$timeStamp]['day'] = $timeStamp;
			$i++;
		}
		
		$counts = array();
		
		foreach($snapshotKeys as $snapshotEntity_id => $snapshotEntity_key)
		{
			// get the
			$counts[$snapshotEntity_key] = $this->UsageEntity->UsageCount->countMatrixRange($snapshotEntity_id, 'day', $start, $end);
			
			foreach($countMatrix as $timeStamp => $zero)
			{
				$out['data'][$timeStamp][$snapshotEntity_key] = (isset($counts[$snapshotEntity_key][$timeStamp])?$counts[$snapshotEntity_key][$timeStamp]:0);
			}
		}
		
		return $out;
	}
	
	public function Snapshot_dynamicEntities(Model $Model)
	{
		$totalStart = time();
		$Model->shellOut(__('Start dynamic Usage Entities for Model %s from database: %s', $Model->alias, $Model->snapshotStats['snapshotFile']), 'snapshot');
		
		// this makes sure the database table exists, if not, then fail nicely.
		try {
			$databaseName = $Model->getDataSource()->config['name'];
		} catch (Exception $e) {
			$Model->shellOut(__('Issue with SnapshotUsageEntity for Model: %s database: %s Message: %s - continuing', $Model->alias, $Model->snapshotStats['snapshotFile'], $e->getMessage()), 'snapshot', 'warning');
			return array();
		}
		
		$Model->shellOut(__('Building dynamic Usage Entities for Model %s from database: %s (%s)', $Model->alias, $Model->snapshotStats['snapshotFile'], $databaseName), 'snapshot');
		
		// build the dynamic entities based on the belongsTo combinations
		if(!isset($Model->snapshotStats['SnapshotUsageEntity']))
		{
			$Model->shellOut(__('Issue with SnapshotUsageEntity for Model: %s database:  %s (%s)', $Model->alias, $Model->snapshotStats['snapshotFile'], $databaseName), 'snapshot', 'warning');
			return array();
		}
		
		$Model->needsBackCompile = false;
		
		if(!isset($Model->getDataSource()->config['name']) or $Model->getDataSource()->config['name'] != 'default')
		{
			$dataSource = $Model->getDataSource();
		}
		elseif(!$dataSource = $Model->snapshotStats['SnapshotUsageEntity']->getSnapshotDataSource('snapshot_'. basename($Model->snapshotStats['snapshotPath'])))
		{
			$Model->shellOut(__('Issue with dataSource (1) for Model: %s database: %s (%s)', $Model->alias, $Model->snapshotStats['snapshotFile'], $databaseName), 'snapshot', 'warning');
			return array();
		}
		
		// make sure the table for this source and model exists
		$tables = $dataSource->listSources();
		if(!in_array($Model->useTable, $tables)) 
		{
			$Model->shellOut(__('Table %s doesn\'t exist for Model: %s in database %s (%s) - moving on.', $Model->useTable, $Model->name, $dataSource->config['name'], $databaseName), 'snapshot', 'warning');
			return array();
		}
		
		// switch all of the belongsTo relationships to the same datasource as this $Model
		$belongsTo = $Model->belongsTo;
		
		$itemsCache = array();
		
		// track original schema names
		$origSchemaNames = array();
		$origDataSources = array();
		
		foreach($belongsTo as $btAlias1 => $btSettings)
		{
			if(!isset($btSettings['plugin_snapshot']))
				continue;
			
			if(!is_array($btSettings['plugin_snapshot']) and !$btSettings['plugin_snapshot'])
				continue;
			
			if(isset($btSettings['plugin_snapshot']['disabled']) and !$btSettings['plugin_snapshot']['disabled'])
				continue;
			
			$finder_options = array();
			if(isset($btSettings['plugin_snapshot']['finder_options']) and is_array($btSettings['plugin_snapshot']['finder_options']))
				$finder_options = $btSettings['plugin_snapshot']['finder_options'];
			
			if(!isset($itemsCache[$btAlias1]))
				$itemsCache[$btAlias1] = $Model->{$btAlias1}->find('list', $finder_options);
			
			$origSchemaNames[$btAlias1] = $Model->{$btAlias1}->schemaName;
			$origDataSources[$btAlias1] = $Model->{$btAlias1}->useDbConfig;
		
			if($Model->{$btAlias1}->schemaName and $Model->{$btAlias1}->schemaName != $dataSource->config['database'])
			{
				$snapshotOtherPath = $Model->snapshotStats['SnapshotUsageEntity']->sqliteFolder. DS. $Model->{$btAlias1}->schemaName. '_'. $Model->snapshotStats['timestamps'][0].'.sqlite';
				
				if(!file_exists($snapshotOtherPath))
				{
					$Model->shellOut(__('Unknown File for snapshot database -  Model: %s database: %s (%s)', $Model->alias, $snapshotOtherPath, $databaseName), 'snapshot', 'warning');
					continue;
				}
				
				$Model->snapshotStats['SnapshotUsageEntity']->createSnapshotDataSource($snapshotOtherPath);
				if(!$dataSource = $Model->snapshotStats['SnapshotUsageEntity']->getSnapshotDataSource('snapshot_'. basename($snapshotOtherPath)))
				{
					$Model->shellOut(__('Issue with dataSource (2) for Model: %s database: %s', $Model->alias, $Model->snapshotStats['snapshotFile']), 'snapshot', 'warning');
					continue;
				}
			}
			else
			{
				if(!$dataSource = $Model->snapshotStats['SnapshotUsageEntity']->getSnapshotDataSource('snapshot_'. basename($Model->snapshotStats['snapshotPath'])))
				{
					$Model->shellOut(__('Issue with dataSource (3) for Model: %s database: ', $Model->alias, $Model->snapshotStats['snapshotFile']), 'snapshot', 'warning');
					continue;
				}
			}
			
			// make sure the table for this model exists in the database
			$tables = $dataSource->listSources();
			if(!in_array($Model->{$btAlias1}->useTable, $tables)) 
			{
				$Model->shellOut(__('Table %s doesn\'t exist for Model: %s in database %s - moving on.', $Model->{$btAlias1}->useTable, $btAlias1, $dataSource->config['name']), 'snapshot', 'warning');
				continue;
			}
			
			$Model->{$btAlias1}->setDataSource($dataSource->config['name']);
			$Model->{$btAlias1}->cacheQueries = false;
//			$Model->{$btAlias1}->schemaName = 'main';
		}
		
		$totalNow = (time() - $totalStart);
		$Model->shellOut(__('(Time %s) Found %s Usage relationships to count for Model %s', $totalNow, count($itemsCache), $Model->alias), 'snapshot');
		$entityCounts = array();
		
		$entityPrefix = $Model->snapshotStats['entityPrefix'];
		
		$entityKeys = array();
		foreach($itemsCache as $btAlias1 => $btItems1)
		{
			foreach($btItems1 as $btItemKey1 => $btItemName1)
			{
				$entityKey1 = $entityPrefix.$btAlias1.'-'.$btItemKey1;
				
				// make sure the single exists
				if(!isset($entityKeys[$entityKey1]))
					$entityKeys[$entityKey1] = true;
			
				foreach($itemsCache as $btAlias2 => $btItems2)
				{
					if($btAlias1 == $btAlias2)
						continue;
					
					foreach($btItems2 as $btItemKey2 => $btItemName2)
					{
						$entityKey2a = $entityPrefix.$btAlias1.'-'.$btItemKey1.'.'.$btAlias2.'-'.$btItemKey2;
						$entityKey2b = $entityPrefix.$btAlias2.'-'.$btItemKey2.'.'.$btAlias1.'-'.$btItemKey1;
						
						// make sure the combo exists
						if(!isset($entityKeys[$entityKey2a]))
							$entityKeys[$entityKey2a] = true;
						
						if(isset($entityKeys[$entityKey2b]))
							unset($entityKeys[$entityKey2b]);
					}
				}
			}
		}
		
		$timeEntityCheckStart = time();	
		
		if(method_exists($Model, 'snapshopCustomEntities'))
		{
			$entityKeys = $Model->snapshopCustomEntities($entityKeys);
		}
		
		$entityKeysCnt = count($entityKeys);
		$totalNow = (time() - $totalStart);
		$Model->shellOut(__('(Time %s) Found %s Usage Entity Keys for Model %s', $totalNow, $entityKeysCnt, $Model->alias), 'snapshot');
		
		$existingEntityIds = $Model->snapshotStats['SnapshotUsageEntity']->existingIds(array_keys($entityKeys), 'snapshot');
		$totalNow = (time() - $totalStart);
		$Model->shellOut(__('(Time %s) Found %s Existing Usage Entity Keys for Model %s (%s)', $totalNow, count($existingEntityIds), $Model->alias, $Model->snapshotStats['snapshotFile']), 'snapshot');
		
		$updateMsg = 0;
		
		foreach($entityKeys as $entityKey => $blah)
		{
			$updateMsg++;
			if ($updateMsg % 100 == 0)
			{
				$percent = round( ($updateMsg / $entityKeysCnt) * 100);
				$totalNow = (time() - $totalStart);
				$timeEntityCheckNow = (time() - $timeEntityCheckStart);
				$Model->shellOut(__('(Time %s/%s) Processed (%s%) %s/%s Usage Entity Keys for Model %s (%s)', $totalNow, $timeEntityCheckNow, $percent, $updateMsg, $entityKeysCnt, $Model->alias, $Model->snapshotStats['snapshotFile']), 'snapshot');
			}
			
			$entityId = false;
			$entityKeyFixed = strtolower(Inflector::underscore(trim($entityKey)));
			if(array_key_exists($entityKeyFixed, $existingEntityIds))
			{
				$entityId = $existingEntityIds[$entityKeyFixed];
			}
			else
			{
				$entityId = $Model->snapshotStats['SnapshotUsageEntity']->checkAdd($entityKey, 'snapshot', array(
					'dynamic' => true,
					'type' => 'snapshot',
				));
				$existingEntityIds[$entityKeyFixed] = $entityId;
			}
				
			if($Model->snapshotStats['SnapshotUsageEntity']->checkAddCreated)
				$Model->needsBackCompile = true;
			
			// split up the key
			$entityKeyParts = explode('.', $entityKey);
			
			// this is from a plugin
			$pluginName = false;
			$modelName = false;
			if($entityKeyParts[0] != $Model->alias)
			{
				$pluginName = array_shift($entityKeyParts).'.';
			}
			
			// single
			$criteria = [];
			if(is_array($blah))
			{
				$criteria = $blah;
			}
			
			if(count($entityKeyParts) == 2)
			{
				list($modelName, $btAlias1) = $entityKeyParts;
				list($btAlias1, $btItemKey1) = explode('-', $btAlias1);
				$criteria1 = $belongsTo[$btAlias1];
				$criteria['conditions'][$Model->alias.'.'.$criteria1['foreignKey']] = $btItemKey1;
			}
			// double
			elseif(count($entityKeyParts) == 3)
			{
				list($modelName, $btAlias1, $btAlias2) = $entityKeyParts;
				list($btAlias1, $btItemKey1) = explode('-', $btAlias1);
				
				if(isset($belongsTo[$btAlias1]))
				{
					$criteria1 = $belongsTo[$btAlias1];
					$criteria['conditions'][$Model->alias.'.'.$criteria1['foreignKey']] = $btItemKey1;
				}
				list($btAlias2, $btItemKey2) = explode('-', $btAlias2);
				if(isset($belongsTo[$btAlias2]))
				{
					$criteria2 = $belongsTo[$btAlias2];
					$criteria['conditions'][$Model->alias.'.'.$criteria2['foreignKey']] = $btItemKey2;
				}
			}
			else
			{
				continue;
			}
			
			if(isset($this->settings[$Model->alias]['countCriteria']) and is_array($this->settings[$Model->alias]['countCriteria']))
			{
				foreach($this->settings[$Model->alias]['countCriteria'] as $criteriaK => $criteriaV)
				{
					if(!isset($criteria[$criteriaK]))
						$criteriaV = array();
					$criteria[$criteriaK] = array_merge($criteriaV, $criteria[$criteriaK]);
				}
			}
			
			$entityCount = $Model->find('count', $criteria);
			
			foreach($Model->snapshotStats['timePeriods'] as $timePeriod => $timeFormat)
			{
				$this_timestamp = $Model->snapshotStats['timestamps'][$timePeriod];
				
				if(!isset($entityCounts[$entityKey.'^'.$timePeriod.'^'.$this_timestamp]))
				{ 
					$entityCounts[$entityKey.'^'.$timePeriod.'^'.$this_timestamp] = array(
						'entityCount' => 0,
						'entityKey' => $entityKey,
						'pluginName' => $pluginName,
						'modelName' => $modelName,
						'timePeriod' => $timePeriod,
						'timeStamp' => $this_timestamp,
					); 
				}
				
				$entityCounts[$entityKey.'^'.$timePeriod.'^'.$this_timestamp]['entityCount'] = ($entityCount?$entityCount:0);
			}
		}
		
		$totalNow = (time() - $totalStart);
		$timeEntityCheckNow = (time() - $timeEntityCheckStart);
		$Model->shellOut(__('(Time %s/%s) Compiled %s Entity Counts in for Model %s (%s)', $totalNow, $timeEntityCheckNow, count($entityCounts), $Model->alias, $Model->snapshotStats['snapshotFile']), 'snapshot');
		
		if($entityCounts)
		{
			ksort($entityCounts);
			$totalNow = (time() - $totalStart);
			$timeCountUpdateStart = time();
			$entityCountsCnt = count($entityCounts);
			$Model->shellOut(__('(Time %s) Updating %s Entity Counts for Model %s (%s)', $totalNow, $entityCountsCnt, $Model->alias, $Model->snapshotStats['snapshotFile']), 'snapshot');
			
			$updateMsg = 0;
			foreach($entityCounts as $entityKey => $entitySettings)
			{
				$updateMsg++;
				if ($updateMsg % 100 == 0)
				{
					$percent = round( ($updateMsg / $entityCountsCnt) * 100);
					$totalNow = (time() - $totalStart);
					$timeCountUpdateNow = (time() - $timeCountUpdateStart);
					$Model->shellOut(__('(Time %s/%s) updateCount (%s%) %s/%s Usage Entity Counts for Model %s (%s)', $totalNow, $timeCountUpdateNow, $percent, $updateMsg, $entityCountsCnt, $Model->alias, $Model->snapshotStats['snapshotFile']), 'snapshot');
				}
				list($entityKey, $timePeriod, $timeStamp) = explode('^', $entityKey);
				extract($entitySettings);
				
				$entityId = false;
				$entityKeyFixed = strtolower(Inflector::underscore(trim($entityKey)));
				if(array_key_exists($entityKeyFixed, $existingEntityIds))
				{
					$entityId = $existingEntityIds[$entityKeyFixed];
				}
				$Model->snapshotStats['SnapshotUsageEntity']->updateCount($entityKey, 'snapshot', $entityCount, $pluginName.$modelName, $timePeriod, $timeStamp, true, $entityId);
			}
			
			$totalNow = (time() - $totalStart);
			$timeCountUpdateNow = (time() - $timeCountUpdateStart);
			$Model->shellOut(__('(Time %s/%s) Updated %s Entity Counts for Model %s (%s)', $totalNow, $timeCountUpdateNow, $entityCountsCnt, $Model->alias, $Model->snapshotStats['snapshotFile']), 'snapshot');
		}
		
		// now that we're done, restore the original datasource and schema name.
		// this way, if we come across this model again, it will be treated like the first time
		foreach($belongsTo as $btAlias1 => $btSettings)
		{
			if(isset($origDataSources[$btAlias1]))
			{
				$Model->{$btAlias1}->setDataSource($origDataSources[$btAlias1]);
			}
			
			if(isset($origSchemaNames[$btAlias1]))
			{
				$Model->{$btAlias1}->schemaName = $origSchemaNames[$btAlias1];
			}
		}
		
		$totalNow = (time() - $totalStart);
		$Model->shellOut(__('(Time %s) Updated all Usage Entities for Model %s (%s)', $totalNow, $Model->alias, $Model->snapshotStats['snapshotFile']), 'snapshot');
		return $entityCounts;
	}
	
	public function loadUsageEntity()
	{
		if(!$this->UsageEntity or $this->UsageEntity instanceof AppModel)
		{
			// reload the UsageCount
			App::uses('UsageEntity', 'Usage.Model');
			$this->UsageEntity = new UsageEntity();
		}
	}
	
	public function snapshotDashboardGetStats(Model $Model, $snapshotKeyRegex = false, $start = false, $end = false)
	{
		return $this->Snapshot_dashboardStats($Model, $snapshotKeyRegex, $start, $end);
	}
	
	public function snapshotStats(Model $Model)
	{
		$entities = $this->Snapshot_dynamicEntities($Model);
		return [];
	}
}
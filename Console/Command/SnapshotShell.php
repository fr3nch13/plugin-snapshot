<?php

App::uses('SnapshotAppShell', 'Snapshot.Console/Command');

class SnapshotShell extends SnapshotAppShell
{
	// the models to use
	public $uses = array('User', 'Snapshot.SnapshotUsageEntity');
	
	public $tasks = array('Utilities.Out');
	
	public function startup() 
	{
		$this->clear();
		$this->out('Snapshot Shell');
		$this->hr();
		return parent::startup();
	}
	
	public function getOptionParser()
	{
	/*
	 * Parses out the options/arguments.
	 * http://book.cakephp.org/2.0/en/console-and-shells.html#configuring-options-and-generating-help
	 */
	
		$parser = parent::getOptionParser();
		
		$parser->description(__d('cake_console', 'The Snapshot Shell used to run cron jobs common in all of the apps.'));
		
		$parser->addSubcommand('take_snapshot', array(
			'help' => __d('cake_console', 'Takes a snapshot of the mysql database, and saves it in sqlite.'),
			'parser' => array(
			),
		));
		
		$parser->addSubcommand('new_entities', [
			'help' => __d('cake_console', 'Adds any new entities not found in the database.'),
			'parser' => [
				'arguments' => [
					'model' => ['help' => __d('cake_console', 'The model to check.')],
				],
			],
		]);
		
		$parser->addSubcommand('update_entities', [
			'help' => __d('cake_console', 'Update the entities from the snapshots.'),
			'parser' => [
				'arguments' => [
					'model' => ['help' => __d('cake_console', 'The model to check.')],
				],
			],
		]);
		
		$parser->addSubcommand('update_stats', [
			'help' => __d('cake_console', 'Update the stats defined in the individual models, for snapshots.'),
			'parser' => [
				'arguments' => [
					'model' => ['help' => __d('cake_console', 'The model to check.')],
				],
			],
		]);
		
		$parser->addSubcommand('recompile_stats', [
			'help' => __d('cake_console', 'Update the stats defined in the individual models, for snapshots.'),
			'parser' => [
				'options' => [
					'date' => ['short' => 'd', 'help' => __d('cake_console', 'The snapshot date in regex format.')],
					'model' => ['short' => 'm', 'help' => __d('cake_console', 'The model(s) to check. Separate each model by a comma.')],
				],
			],
		]);
		
		return $parser;
	}
	
	public function take_snapshot_old()
	{
		$results = $this->SnapshotUsageEntity->takeSnapshot();
		$this->out(__('Results: %s', $results));
	}
	
	public function take_snapshot()
	{
		$this->out('take_snapshot2');
		$results = $this->Tasks->load('Snapshot.TakeSnapshot')->execute($this);
	}
	
	public function new_entities()
	{
		Configure::write('debug', 1);
		
		$modelFilter = false;
		if(isset($this->args[0]))
			$modelFilter = $this->args[0];
		
		// check the SnapshotUsageEntity
		if(!isset($this->SnapshotUsageEntity) or !$this->SnapshotUsageEntity or $this->SnapshotUsageEntity instanceof AppModel)
		{
			// reload the SnapshotUsageEntity
			App::uses('SnapshotUsageEntity', 'Snapshot.Model');
			$this->SnapshotUsageEntity = new SnapshotUsageEntity();
		}
		
		// make sure the statically defined entities are in the database
		$results = $this->SnapshotUsageEntity->checkEntities();
		$this->out(__('%s::checkEntities() Results: %s', $this->SnapshotUsageEntity->alias, count($results)));
		
		// update the entities
		$results = $this->SnapshotUsageEntity->updateEntities(true);
		$this->out(__('%s::updateEntities() Results: %s', $this->SnapshotUsageEntity->alias, $results));
	}
	
	public function update_entities()
	{
		Configure::write('debug', 1);
		
		// check the SnapshotUsageEntity
		if(!isset($this->SnapshotUsageEntity) or !$this->SnapshotUsageEntity or $this->SnapshotUsageEntity instanceof AppModel)
		{
			// reload the SnapshotUsageEntity
			App::uses('SnapshotUsageEntity', 'Snapshot.Model');
			$this->SnapshotUsageEntity = new SnapshotUsageEntity();
		}
		
		// make sure the statically defined entities are in the database
		$results = $this->SnapshotUsageEntity->checkEntities();
		$this->out(__('%s::checkEntities() Results: %s', $this->SnapshotUsageEntity->alias, count($results)));
		
		// update the entities
		$results = $this->SnapshotUsageEntity->updateEntities();
		$this->out(__('%s::updateEntities() Results: %s', $this->SnapshotUsageEntity->alias, $results));
	}
	
	public function update_stats()
	{
		Configure::write('debug', 1);
		
		// check the SnapshotUsageEntity
		if(!isset($this->SnapshotUsageEntity) or !$this->SnapshotUsageEntity or $this->SnapshotUsageEntity instanceof AppModel)
		{
			// reload the SnapshotUsageEntity
			App::uses('SnapshotUsageEntity', 'Snapshot.Model');
			$this->SnapshotUsageEntity = new SnapshotUsageEntity();
		}
		
		// update the entities
		$results = $this->SnapshotUsageEntity->updateStats();
		$this->out(__('%s::updateStats() Results: %s', $this->SnapshotUsageEntity->alias, $results));
	}
	
	public function recompile_stats()
	{
		Configure::write('debug', 1);
		
		$finder = false; // filter for which sqlite files to run on, generally it should be a date like 2016-09-02
		if($this->param('date'))
			$finder = $this->param('date');
		
		$modelFilters = false;
		if($this->param('model'))
			$modelFilters = $this->param('model');
		
		// check the SnapshotUsageEntity
		if(!isset($this->SnapshotUsageEntity) or !$this->SnapshotUsageEntity or $this->SnapshotUsageEntity instanceof AppModel)
		{
			// reload the SnapshotUsageEntity
			App::uses('SnapshotUsageEntity', 'Snapshot.Model');
			$this->SnapshotUsageEntity = new SnapshotUsageEntity();
		}
		
		// update the entities
		$results = $this->SnapshotUsageEntity->updateStats(true, $finder, $modelFilters);
		$this->out(__('%s::updateStats(true, %s) Results: %s', $this->SnapshotUsageEntity->alias, $finder, $results));
	}
}
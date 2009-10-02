<?php
/*
 * Importing needed Classes
 */
App::import('Vendor', 'Service', array('file' => 'Solr' . DS . 'Service.php'));
App::import('Helper', 'Time');
/**
 * SolrIndexable Behavior
 * 
 * Automatically indexes or deletes data of a Model on a solr matching engine.
 * 
 * This Behavior is only indexing. A Solr Query Datasource will soon be available too.
 * 
 * @package app
 * @subpackage app.models.behaviors
 * @author Thomas Ploch
 */
class SolrIndexableBehavior extends ModelBehavior {
/**
 * Mapping table for cake's integrated field types
 * These map to solr's default dynamic field definitions
 * 
 * Related Model IDs will get mapped to a multi value dynamic field '*_mi'
 * 
 * @var array
 */
	var $__fieldTypeMappingTable = array(
		'integer' => '_i',
		'string' => '_s',
		'boolean' => '_b',
		'float' => '_f',
		'datetime' => '_dt',
		'date' => '_dt',
		'timestamp' => '_dt',
		'text' => '_t',
		'relations' => '_mi'
	);
/**
 * Holds the boost values for certain fields specified in settings
 * 
 * @var array
 * @access private
 */
	var $__boostTable = array();
/**
 * The translated Schema
 * 
 * @var array
 * @access private
 */
	var $__transSchema = array();
/**
 * The field in which the Model alias will be saved
 * 
 * @var array
 * @access private
 */
	var $__fieldModelAlias = 'model_s';
	/**
 * The field in which the Model Id will be saved
 * 
 * @var array
 * @access private
 */
	var $__fieldModelId = 'foreignKey';
/**
 * The Behavior setup function.
 * Initializes settings and creates needed Objects
 * 
 * @param object $Model
 * @param object $settings
 * @return void
 * @access private
 */
	function setup(&$Model, $settings) {
		$this->__initSettings($Model, $settings);
		$this->__initTransSchema();
		$this->__createBoostTable();
		$this->__initObjects();
	}
/**
 * Initializes the Behavior Settings
 * 
 * @param object $Model
 * @param array $settings
 * @return void
 * @access private
 */
	function __initSettings(&$Model, $settings) {
		$this->model = $Model;
		if (!isset($this->settings[$this->model->alias])) {
			$this->settings[$this->model->alias] = array(
				'host' => 'localhost',
				'port' => 8080,
				'path' => '/solr/',
				'include' => array(),
				'fields' => array(),
				'boost' => false
			);
		}
		$this->settings[$this->model->alias] = am($this->settings[$this->model->alias], (array)$settings);
	}
/**
 * Initializes a TimeHelper and a SolR Service object
 * 
 * @return void
 * @access private
 */
	function __initObjects() {
		$settings = $this->settings[$this->model->alias];
		$this->time = new TimeHelper();
		$this->solr = new Apache_Solr_Service($settings['host'], $settings['port'], $settings['path']);
	}
/**
 * Creates a table to look up boost values
 * 
 * @return void
 * @access private
 */
	function __createBoostTable() {
		foreach ($this->settings[$this->model->alias]['fields'] as $fieldname => $value) {
			if (is_array($value) && isset($value['boost'])) {
				$this->__boostTable[$fieldname] = $value['boost'];
			}
		}
	}
/**
 * Returns an array $fieldname => $translatedField
 * 
 * @param object Model &$Model The Model to which the Behavior was attached
 * @param string $fieldname
 * @param array $descArray Description array for field $fieldname
 * @return array
 * @access private
 */
	function __setSchemaField($fieldname, $descArray) {
		$result = array();
		if ($fieldname == $this->model->primaryKey) {
			$result[$fieldname] = $this->__fieldModelId . $this->__fieldTypeMappingTable[$descArray['type']];
		} else {
			$result[$fieldname] = $fieldname . $this->__fieldTypeMappingTable[$descArray['type']];
		}
		return $result;
	}
/**
 * Translates the _schema of a given Model
 * 
 * If fields is empty, translates all fields.
 * Else translates only fields specified in $fields
 * 
 * @param object $Model The Model the Behavior is attached to
 * @param array $fields [optional] Fields to be translated and indexed
 * @return array
 * @access private
 */
	function __initTransSchema() {
		$fields = $this->settings[$this->model->alias]['fields'];
		$primary = $this->model->primaryKey;
		$results = array();
		$schema = $this->model->_schema;
		$results = am($results, $this->__setSchemaField($primary, $schema[$primary]));
		foreach ($schema as $fieldname => $descArray) {
			if (!empty($fields)) {
				if (array_key_exists($fieldname, $fields)) {
					$results =  am($results, $this->__setSchemaField($fieldname, $descArray));
				}
			} else {
				if ($fieldname !== $primary) {
					$results = am($results, $this->__setSchemaField($fieldname, $descArray));
				}
			}
		}
		$this->__transSchema = $results;
	}
/**
 * Creates a solrsafe datestring
 * 
 * @param object $dateString
 * @return string solr safe datestring
 * @access private
 */
	function __makeSolrTime($dateString) {
		$date = $this->time->format('Y-m-d', intval($this->time->fromString($dateString)));
		$date .= 'T';
		$date .= $this->time->format('H:i:s', intval($this->time->fromString($dateString)));
		$date .= 'Z';
		return $date;
	}
/**
 * Creates a unique solr Document id for a given Model.
 * Format: Model_12 (Model->alias . '_' . Model->id)
 * 
 * @return string Document id
 * @access private
 */
	function __createDocId() {
		return $this->model->alias . '_' . $this->model->id;
	}
/**
 * Safely adds a document to solr index.
 * On error continues, but logs the error to solr.log
 * 
 * @param solr Document $document
 * @return void
 */
	function __safeAddDocument($document) {
		try {
			$this->solr->addDocument($document);
			$this->solr->commit();
			$this->solr->optimize();
		} catch (Exception $e) {
			$this->log($e, 'solr');
		}
	}
/**
 * Safely deletes a document from solr index.
 * Continues on error, but logs error to solr.log
 * 
 * @param object Model $Model
 * @return void
 */
	function __safeDeleteDocument() {
		try {
			$this->solr->deleteById($this->__createDocId());
			$this->solr->commit();
			$this->solr->optimize();	
		} catch (Exception $e) {
			$this->log($e, 'solr');
		}
	}
/**
 * Processes a data array and sets the corresponding fields in the solr document
 * 
 * @param solr document object $document
 * @param array $data
 * @return solr document object
 */
	function __processDocumentFieldArray($document, $data) {
		foreach ($data as $fieldname => $value) {
			if (!isset($this->__transSchema[$fieldname])) {
				continue;
			} else {
				if (preg_match('/.*_dt/u', $this->__transSchema[$fieldname])) {
					$document->setField($this->__transSchema[$fieldname], $this->__makeSolrTime($value));
				} else {
					$document->setField($this->__transSchema[$fieldname], $value);
				}
			}
			if (array_key_exists($fieldname, $this->__boostTable)) {
				$document->setFieldBoost($this->__transSchema[$fieldname], $this->__boostTable[$fieldname]);
			}
		}
		return $document;
	}
/**
 * Processes the related Model data in MutliValued fields
 * 
 * @param solr document object $document
 * @param string $field fieldname
 * @param array $data data array
 * @return solr document object
 */
	function __processDocumentRelationalFieldArray($document, $field, $data) {
		foreach ($data as $item) {
			foreach($item as $n => $value) {
				$document->setMultiValue($field, intval($value), false);
			}
		}
		return $document;
	}
/**
 * Iterates through Model->data and sets the given values for fields found in $this->__transSchema.
 * 
 * @param object $Model
 * @return solr document Object
 */
	function __processDocument() {
		$document = new Apache_Solr_Document;
		if ($this->settings[$this->model->alias]['boost']) {
			$document->setBoost($this->settings[$this->model->alias]['boost']);
		}
		foreach ($this->model->data as $alias => $data) {
			if ($alias === $this->model->alias) {
				$document = $this->__processDocumentFieldArray($document, $data);
			} elseif (in_array($alias, $this->settings[$this->model->alias]['include'])) {
				$field = $alias . $this->__fieldTypeMappingTable['relations'];
				$document = $this->__processDocumentRelationalFieldArray($document, $field, $data);
			}
		}
		$document->setField($this->__fieldModelAlias, $this->model->alias);
		$document->setField('id', $this->__createDocId());
		return $document;
	}
/**
 * Behavior's afterSave callback
 * 
 * @param object $Model
 * @param boolean $created
 * @return boolean
 */
	function afterSave(&$Model, $created) {
		$return = parent::afterSave($Model, $created);
		if (!$created) {
			$this->__safeDeleteDocument();
		}
		$document = $this->__processDocument();
		$this->__safeAddDocument($document);
		return $return;
	}
/**
 * Behavior's beforeDelete callback
 * 
 * @param object $Model
 * @return boolean
 */
	function beforeDelete(&$Model) {
		$return = parent::beforeDelete($Model);
		$this->__safeDeleteDocument();
		return $return;
	}
}
?>
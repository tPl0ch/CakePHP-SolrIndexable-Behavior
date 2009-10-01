<?php
/**
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
 */
	function setup(&$Model, $settings) {
		if (!isset($this->settings[$Model->alias])) {
			$this->settings[$Model->alias] = array(
				'host' => 'localhost',
				'port' => 8080,
				'path' => '/solr/',
				'include' => array(),
				'fields' => array(),
				'boost' => false
			);
		}
		$this->model = $Model->alias;
		$this->settings[$Model->alias] = am($this->settings[$Model->alias], (array)$settings);
		$this->__transSchema = $this->__translateSchema($Model, $this->settings[$Model->alias]['fields']);
		$this->__createBoostTable();
		$settings = $this->settings[$Model->alias];
		$this->time = new TimeHelper();
		$this->solr = new Apache_Solr_Service($settings['host'], $settings['port'], $settings['path']);
	}

	function __createBoostTable() {
		foreach ($this->settings[$this->model]['fields'] as $fieldname => $value) {
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
	function __setSchemaField(&$Model, $fieldname, $descArray) {
		$result = array();
		if ($fieldname == $Model->primaryKey) {
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
 */
	function __translateSchema(&$Model, $fields = array()) {
		$results = array();
		$schema = $Model->_schema;
		foreach ($schema as $fieldname => $descArray) {
			if (!empty($fields)) {
				if (array_key_exists($fieldname, $fields)) {
					$results =  am($results, $this->__setSchemaField($Model, $fieldname, $descArray));
				}
			} else {
				$results = am($results, $this->__setSchemaField($Model, $fieldname, $descArray));
			}
		}
		return $results;
	}
/**
 * Creates a solrsafe datestring
 * 
 * @param object $dateString
 * @return string solr safe datestring 
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
 * @param object $Model
 * @return string Document id
 */
	function __createDocId(&$Model) {
		return $Model->alias . '_' . $Model->id;
	}
/**
 * Safely adds a document to solr index.
 * On error continues, but logs the error to solr.log
 * 
 * @param solr Document $document
 * @return void
 */
	function __saveAddDocument($document) {
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
	function __saveDeleteDocument(&$Model) {
		try {
			$this->solr->deleteById($this->__createDocId($Model));
			$this->solr->commit();
			$this->solr->optimize();	
		} catch (Exception $e) {
			$this->log($e, 'solr');
		}
	}
/**
 * 
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
				$document->setFieldBoost($fieldname, $this->__boostTable[$fieldname]);
			}
		}
		return $document;
	}
/**
 * Processes the related
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
	function __processDocument(&$Model) {
		$document = new Apache_Solr_Document;
		foreach ($Model->data as $alias => $data) {
			if ($alias === $Model->alias) {
				$document = $this->__processDocumentFieldArray($document, $data);
			} elseif (in_array($alias, $this->settings[$Model->alias]['include'])) {
				$field = $alias . $this->__fieldTypeMappingTable['relations'];
				$document = $this->__processDocumentRelationalFieldArray($document, $field, $data);
			}
		}
		$document->setField($this->__fieldModelAlias, $Model->alias);
		$document->setField('id', $this->__createDocId($Model));
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
			$this->__saveDeleteDocument($Model);
		}
		$document = $this->__processDocument($Model);
		$this->__saveAddDocument($document);
		$this->log($document);
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
		$this->__saveDeleteDocument($Model);
		return $return;
	}
}
?>
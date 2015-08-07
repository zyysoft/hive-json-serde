/**
 * JSON SerDe for Hive
 */
package org.apache.hadoop.hive.contrib.serde2;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.json.JSONException;
import org.json.JSONObject;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathUtil;

/**
 * JSON SerDe for Hive
 * <p>
 * This SerDe can be used to read data in JSON format. For example, if your JSON
 * files had the following contents:
 * 
 * <pre>
 * {"field1":"data1","field2":100,"field3":"more data1"}
 * {"field1":"data2","field2":200,"field3":"more data2"}
 * {"field1":"data3","field2":300,"field3":"more data3"}
 * {"field1":"data4","field2":400,"field3":"more data4"}
 * </pre>
 * 
 * The following steps can be used to read this data:
 * <ol>
 * <li>Build this project using <code>ant build</code></li>
 * <li>Copy <code>hive-json-serde.jar</code> to the Hive server</li>
 * <li>Inside the Hive client, run
 * 
 * <pre>
 * ADD JAR /home/hadoop/hive-json-serde.jar;
 * </pre>
 * 
 * </li>
 * <li>Create a table that uses files where each line is JSON object
 * 
 * <pre>
 * CREATE EXTERNAL TABLE IF NOT EXISTS my_table (
 *    field1 string, field2 int, field3 string
 * )
 * ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.JsonSerde'
 * LOCATION '/my_data/my_table/';
 * </pre>
 * 
 * </li>
 * <li>Copy your JSON files to <code>/my_data/my_table/</code>. You can now
 * select data using normal SELECT statements
 * 
 * <pre>
 * SELECT * FROM my_table LIMIT 10;
 * </pre>
 * 
 * 
 * </li>
 * </ol>
 * <p>
 * The table does not have to have the same columns as the JSON files, and
 * vice-versa. If the table has a column that does not exist in the JSON object,
 * it will have a NULL value. If the JSON file contains fields that are not
 * columns in the table, they will be ignored and not visible to the table.
 * 
 * 
 * @see <a href="http://code.google.com/p/hive-json-serde/">hive-json-serde on
 *      Google Code</a>
 * @author Peter Sankauskas
 */
public class JsonSerde implements SerDe {
	/**
	 * Apache commons logger
	 */
	private static final Log LOG = LogFactory.getLog(JsonSerde.class.getName());

	/**
	 * The number of columns in the table this SerDe is being used with
	 */
	private int numColumns;

	/**
	 * List of column names in the table
	 */
	private List<String> columnNames;

	/**
	 * An ObjectInspector to be used as meta-data about a deserialized row
	 */
	private StructObjectInspector rowOI;

	/**
	 * List of row objects
	 */
	private ArrayList<Object> row;

	/**
	 * List of column type information
	 */
	private List<TypeInfo> columnTypes;

	private Properties tbl;
	// Build a hashmap from column name to JSONPath expression.
	private Map<String, JsonPath> colNameJsonPathMap;
	private StructTypeInfo docTypeInfo;
	
	/**
	 * Initialize this SerDe with the system properties and table properties
	 * 
	 */
	@Override
	public void initialize(Configuration sysProps, Properties tblProps)
			throws SerDeException {
		LOG.debug("Initializing JsonSerde");

		tbl = tblProps;

		// Get the names of the columns for the table this SerDe is being used
		String columnNameProperty = tblProps
				.getProperty(Constants.LIST_COLUMNS);
		columnNames = Arrays.asList(columnNameProperty.split(","));
		
		// Convert column types from text to TypeInfo objects
		String columnTypeProperty = tblProps
				.getProperty(Constants.LIST_COLUMN_TYPES);
		// Get the table column types
		columnTypes = TypeInfoUtils
				.getTypeInfosFromTypeString(columnTypeProperty);
		/**
		 * Make sure the number of column types and column names are equal.
		 */
		/*
		 * assert columnNames.size() == columnTypes.size();
		 * */
		numColumns = columnNames.size();
		// Get the structure and object inspector
		docTypeInfo =
		            (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
		/**
		 * Store the mapping between the column name and the JSONPath expression
		 * for accessing that column's value within the JSON object.
		 */
		colNameJsonPathMap = new HashMap<String, JsonPath>();
		/*
		 * String[] propertiesSet = new
		 * String[tblProps.stringPropertyNames().size()]; propertiesSet =
		 * tblProps.stringPropertyNames().toArray(propertiesSet);
		 */
		
		String currentJsonPath;
		int z = 0;
 		for (String colName : columnNames) {
			LOG.debug("Iteration #" + z);
			currentJsonPath = null;
			 // for (int i = 0; i < propertiesSet.length; i++) {
			 // LOG.debug("current property=" + propertiesSet[i]); if
			 //(propertiesSet[i].equalsIgnoreCase(colName)) { currentJsonPath =
			 // tblProps.getProperty(propertiesSet[i]); break; } }
			 
			currentJsonPath = tbl.getProperty(colName);
			if (currentJsonPath == null) {
				String errorMsg = ("SERDEPROPERTIES must include a property for every column. "
						+ "Missing property for column '" + colName + "'.");
				LOG.error(errorMsg);
				throw new SerDeException(errorMsg);
			}

			LOG.info("Checking JSON path=" + currentJsonPath);
			if (!PathUtil.isPathDefinite(currentJsonPath)) {
				throw new SerDeException(
						"All JSON paths must point to exaclty one item. "
								+ " The following path is ambiguous: "
								+ currentJsonPath);
			}
			
			LOG.info("fieldName:"+colName+";fieldJsonPath:"+currentJsonPath+";fieldType:"+docTypeInfo.getStructFieldTypeInfo(colName).getTypeName());
			 
			LOG.debug("saving json path=" + currentJsonPath);
			// @todo consider trimming the whitespace from the tokens.
			colNameJsonPathMap.put(colName, JsonPath.compile(currentJsonPath));
			z++;
		}
		
		// Create ObjectInspectors from the type information for each column
		List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(
				columnNames.size());
		ObjectInspector oi;
		for (int c = 0; c < numColumns; c++) {
			oi = TypeInfoUtils
					.getStandardJavaObjectInspectorFromTypeInfo(columnTypes
							.get(c));
			columnOIs.add(oi);
		}
		rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(
				columnNames, columnOIs);
		// Create an empty row object to be reused during deserialization
		row = new ArrayList<Object>(numColumns);
		/*for (int c = 0; c < numColumns; c++) {
			row.add(null);
		}
		LOG.info("row:.........."+row.size());*/
		
		LOG.debug("JsonSerde initialization complete");
	}

	
	/**
	 * Gets the ObjectInspector for a row deserialized by this SerDe
	 */
	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return rowOI;
	}

	/**
	 * Deserialize a JSON Object into a row for the table
	 */
	public Object deserialize_old(Writable blob) throws SerDeException {
		Text rowText = (Text) blob;
		LOG.debug("Deserialize row: " + rowText.toString());

		// Try parsing row into JSON object
		JSONObject jObj;
		try {
			jObj = new JSONObject(rowText.toString()) {
				/**
				 * In Hive column names are case insensitive, so lower-case all
				 * field names
				 * 
				 * @see org.json.JSONObject#put(java.lang.String,
				 *      java.lang.Object)
				 */
				@Override
				public JSONObject put(String key, Object value)
						throws JSONException {
					// return super.put(key.toLowerCase(), value);
					return super.put(key, value);
				}
			};
		} catch (JSONException e) {
			// If row is not a JSON object, make the whole row NULL
			LOG.error("Row is not a valid JSON Object - JSONException: "
					+ e.getMessage());
			return null;
		}

		LOG.debug("JSON row looks like: " + jObj.toString());

		// Loop over columns in table and set values
		JsonPath colPath;
		String jsonStr = jObj.toString();
		Object tmpValue;
		String colValue;
		Object value = null;
		for (int c = 0; c < numColumns; c++) {
			LOG.debug("Processing column #" + c + ". col name="
					+ columnNames.get(c));
			colPath = colNameJsonPathMap.get(columnNames.get(c));
			TypeInfo ti = columnTypes.get(c);

			try {
				tmpValue = colPath.read(jsonStr);
			} catch (ParseException e) {
				throw new SerDeException(e.toString());
			}

			if (tmpValue == null) {
				/*
				 * LOG.warn(
				 * "No JSON value was found for the JSON path associated with column '"
				 * + columnNames.get(c) + "' or the value was 'null'.");
				 */
				value = null;
			} else {
				colValue = tmpValue.toString();
				// Get type-safe JSON values
				if (ti.getTypeName().equalsIgnoreCase(
						Constants.DOUBLE_TYPE_NAME)) {
					value = "".equals(colValue.trim()) ? null : Double
							.valueOf(colValue);
				} else if (ti.getTypeName().equalsIgnoreCase(
						Constants.BIGINT_TYPE_NAME)) {
					value = "".equals(colValue.trim()) ? null : Long
							.valueOf(colValue);
				} else if (ti.getTypeName().equalsIgnoreCase(
						Constants.INT_TYPE_NAME)) {
					value = "".equals(colValue.trim()) ? null : Integer
							.valueOf(colValue);
				} else if (ti.getTypeName().equalsIgnoreCase(
						Constants.TINYINT_TYPE_NAME)) {
					value = "".equals(colValue.trim()) ? null : Byte
							.valueOf(colValue);
				} else if (ti.getTypeName().equalsIgnoreCase(
						Constants.FLOAT_TYPE_NAME)) {
					value = "".equals(colValue.trim()) ? null : Float
							.valueOf(colValue);
				} else if (ti.getTypeName().equalsIgnoreCase(
						Constants.BOOLEAN_TYPE_NAME)) {
					value = "".equals(colValue.trim()) ? null : Boolean
							.valueOf(colValue);
				}
				
				else {
					// Fall back, just get an object
					value = colValue;
				}
			}

			row.set(c, value);
		}

		return row;
	}
	
	
	 @Override
	//CHECKSTYLE:OFF
	public Object deserialize(final Writable writable) throws SerDeException {
		//清空一行数据
		row.clear();
		Text rowText = (Text) writable;
//		LOG.debug("Deserialize row: " + rowText.toString());

		// Try parsing row into JSON object
		JSONObject jObj;
		try {
			jObj = new JSONObject(rowText.toString()) {
				@Override
				public JSONObject put(String key, Object value)
						throws JSONException {
					return super.put(key, value);
				}
			};
		} catch (JSONException e) {
			// If row is not a JSON object, make the whole row NULL
			LOG.error("Row is not a valid JSON Object - JSONException: "
					+ e.getMessage());
			return null;
		}
 
        // For each field, cast it to a HIVE type and add to the current row
		JsonPath fieldJsonPath;
		Object value;
        List<String> structFieldNames = docTypeInfo.getAllStructFieldNames();
        for (String fieldName : structFieldNames) {
            try {
            	fieldJsonPath = colNameJsonPathMap.get(fieldName);
                TypeInfo fieldTypeInfo = docTypeInfo.getStructFieldTypeInfo(fieldName);
                
                LOG.debug("deserialize.........fieldName:"+fieldName+";fieldJsonPath:"+fieldJsonPath.toString()+";fieldType:"+fieldTypeInfo.getTypeName());
                
                value = deserializeField(fieldJsonPath.read(jObj.toString()), fieldTypeInfo, fieldName);
            } catch (Exception e) {
                LOG.warn("Could not find the appropriate field for name " + fieldName);
                LOG.warn(e.getMessage());
                value = null;
            }
            row.add(value);
        }
        return row;
    }
	 
	//json值转换成hive中的数据类型
    public Object deserializeField(final Object value, final TypeInfo valueTypeInfo, final String ext) throws Exception {
    	LOG.debug("deserializeField...........value:"+value+",typeInfo:"+valueTypeInfo.getCategory().name()+",ext:"+ext);
    	
        if (value != null) {
            switch (valueTypeInfo.getCategory()) {
            	case PRIMITIVE:
            		return deserializePrimitive(value, (PrimitiveTypeInfo) valueTypeInfo);
                case LIST:
                    return deserializeArray(value, (ListTypeInfo) valueTypeInfo, ext);
                case MAP:
                    return deserializeMap(value, (MapTypeInfo) valueTypeInfo, ext);
                default:
                	return null;
                /*default:
                    return deserializeBasicType(value,valueTypeInfo.getTypeName(),ext);*/
            }
        }
        return null;
    }
    
    
    /**
     * Most primitives are included, but some are specific to Mongo instances
     */
    private Object deserializePrimitive(final Object value, final PrimitiveTypeInfo valueTypeInfo) {
    	Object colValue;
    	LOG.debug("deserializePrimitive..........."+valueTypeInfo.getPrimitiveCategory().name()+",value:"+value);
    	switch (valueTypeInfo.getPrimitiveCategory()) {
          /*  case BINARY:
                return value;*/
            case BOOLEAN:
            	colValue = "".equals(value) ? null : (Boolean) value;
            case DOUBLE:
            	colValue = "".equals(value) ? null : (Double) value;
            case FLOAT:
            	colValue = "".equals(value) ? null : (Float) value;
            case INT:
            	colValue = "".equals(value) ? null : (Integer) value;
               /* if (value instanceof Double) {
                    return ((Double) value).intValue();
                }
                return value;*/
            case LONG:
            	colValue = "".equals(value) ? null : (Long) value;
            case SHORT:
            	colValue = "".equals(value) ? null : (Short) value;
            case STRING:
                return value.toString();
            default:
            	return null;
        /*    case TIMESTAMP:
                if (value instanceof Date) {
                    return new Timestamp(((Date) value).getTime());
                } else if (value instanceof BSONTimestamp) {
                    return new Timestamp(((BSONTimestamp) value).getTime() * 1000L);
                } else {
                    return value;
                }
            default:
                return deserializeMongoType(value);*/
        }
    }
    
    /**
     * Deserialize a List with the same listElemTypeInfo for its elements
     * @throws Exception 
     */
    private Object deserializeArray(final Object value, final ListTypeInfo valueTypeInfo, final String ext) throws Exception {
//        JSONArray list = (JSONArray) value;
        net.minidev.json.JSONArray list = (net.minidev.json.JSONArray) value;
        //ARRAY<STRING>
        TypeInfo listElemTypeInfo = valueTypeInfo.getListElementTypeInfo();
        LOG.debug("deserializeArray......"+ext+":"+value+";"+"elemType:"+listElemTypeInfo.getTypeName());
        for (int i = 0; i < list.size(); i++) {
            list.set(i, deserializeField(list.get(i), listElemTypeInfo, ext));
        }
        return list;
    }
    

    /**
     * Also deserialize a Map with the same mapElemTypeInfo
     * @throws Exception 
     * @throws  
     */
    private Object deserializeMap(final Object value, final MapTypeInfo valueTypeInfo, final String ext) throws Exception {
    	net.minidev.json.JSONObject b = (net.minidev.json.JSONObject) value;
    	Map<String,Object> map = new HashMap<String, Object>();
        TypeInfo mapValueTypeInfo = valueTypeInfo.getMapValueTypeInfo();
        LOG.debug("deserializeMap...........value:"+value+",valueType:"+valueTypeInfo.getCategory().name()+",ext:"+ext);
        for(Entry<String, Object> entry:b.entrySet()){
        	map.put(entry.getKey(), deserializeField(entry.getValue(),mapValueTypeInfo,ext));
        }
        return map;
    }
    
	/**
	 * Not sure - something to do with serialization of data
	 */
	@Override
	public Class<? extends Writable> getSerializedClass() {
		return Text.class;
	}

	/**
	 * Serializes a row of data into a JSON object
	 * 
	 * @todo Implement this - sorry!
	 */
	@Override
	public Writable serialize(Object obj, ObjectInspector objInspector)
			throws SerDeException {
		LOG.info("-----------------------------");
		LOG.info("--------- serialize ---------");
		LOG.info("-----------------------------");
		LOG.info(obj.toString());
		LOG.info(objInspector.toString());

		return null;
	}

	@Override
	public SerDeStats getSerDeStats() {
		// TODO Auto-generated method stub
		return null;
	}

}

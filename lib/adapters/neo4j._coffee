neo4j = require('neo4j')

exports.initialize = (schema, callback) ->
	return if not neo4j

	getDBURL = (schemaSettings) ->
		url: schemaSettings.protocol + '://' + schemaSettings.username + ':' + schemaSettings.password + '@' + schemaSettings.host + ':' + schemaSettings.port +
		'/' + schemaSettings.name,
		port: schemaSettings.port

	s = schema.settings
	if schema.settings.url
		url = require('url').parse(schema.settings.url)
		s.host = url.hostname
		s.port = url.port
		s.database = url.pathname.replace(/^\//, '')
		s.username = url.auth && url.auth.split(':')[0]
		s.password = url.auth && url.auth.split(':')[1]

	s.protocol = s.protocol || 'http'
	s.username = s.username || ''
	s.password = s.password || ''
	s.port = s.port || 7474
	s.host = s.host || 'localhost'
	s.url = s.url || getDBURL(s)

	schema.client = new neo4j.GraphDatabase(s.url)
	schema.adapter = new Neo4j(schema.client)
	schema.adapter.schema = schema
	process.nextTick(callback)

###
 Neo4J adapter
###
class Neo4j
	constructor: (@client) ->
		@_models = {}
		@cache = {}

	define: (descr) ->
		@mixClassMethods(descr.model, descr.properties)
		@mixInstanceMethods(descr.model.prototype, descr.properties)
		@_models[descr.model.modelName] = descr

	createIndexHelper: (cls, indexName) ->
		db = @client
		method = 'findBy' + indexName[0].toUpperCase() + indexName.substr(1)
		cls[method] = (value, _) ->
			node = db.getIndexedNode cls.modelName, indexName, value, _
			if node and node.data
				node.data.id = node.id
				node.data._node = node
				return new cls(node.data)

	mixClassMethods: (cls, properties) ->
		neo = this
		Object.keys(properties).forEach((name) ->
			neo.createIndexHelper(cls, name) if properties[name].index)

		cls.setupCypherQuery = (name, queryStr, rowHandler) ->
			cls[name] = (_, params = []) ->
				params = [params] if params.constructor.name isnt 'Array'
				i = 0;
				q = queryStr.replace(/\?/g, ->
					params[i++])
				neo.client.query(_, q).map_(rowHandler)

		###
		* @param from - id of object to check relation from
		* @param to - id of object to check relation to
		* @param type - type of relation
		* @param direction - all | incoming | outgoing
		* @param _ - callback (err, rel || false)
		###
		cls.relationshipExists = (from, to, type, direction, _) ->
			node = neo.node(from, _)
			node._getRelationships(direction, type, _).forEach_((r) ->
				if (r.start.id is from and r.end.id is to)
					return true;
			)

		cls.createRelationshipTo = (id1, id2, type, data, _) ->
			fromNode = neo.node(id1, _)
			toNode = neo.node(id2, _)
			fromNode(_).createRelationshipTo(toNode(_), type, cleanup(data), _)

		cls.createRelationshipFrom = (id1, id2, type, data, _) ->
			cls.createRelationshipTo id2, id1, type, data, _

		# only create relationship if it is not exists
		cls.ensureRelationshipTo = (id1, id2, type, data, _) ->
			exists = cls.relationshipExists(id1, id2, type, 'outgoing', _)
			cls.createRelationshipTo(id1, id2, type, data, _) if not exists

		cls.getRelationshipByType = (from, type, _) ->
			node = neo.node(from, _)
			node.getRelationships(type, _)

	mixInstanceMethods: (proto) ->
		neo = this;

		###
		* @param obj - Object or id of object to check relation with
		* @param type - type of relation
		* @param _ - callback (err, rel || false)
		###
		proto.isInRelationWith = (obj, type, direction, _) ->
			this.constructor.relationshipExists(this.id, obj.id || obj, type, 'all', _)

	node: (id, _) =>
		return @cache[id] if @cache[id]
		node = @client.getNodeById(id, _)
		@cache[id] = node if node
		return node

	create: (model, data, _) =>
		data.nodeType = model;
		node = @client.createNode();
		node.data = cleanup(data);
		node.data.nodeType = model;
		node.save(_)
		@cache[node.id] = node;
		node.index(model, 'id', node.id, _)
		this.updateIndexes(model, node, _)
		return node

	updateIndexes: (model, node, _) =>
		props = this._models[model].properties
		Object.keys(props).forEach_(_, (_, key) ->
			if (props[key].index and node.data[key])
				node.index(model, key, node.data[key], _)
		)

	save: (model, data, _) =>
		node = this.node(data.id, _)
		#delete id property since that's redundant and we use the node.id
		delete data.id;
		node.data = cleanup(data);
		node.save(_)
		this.updateIndexes(model, node, _)
		#map node id to the id property being sent back
		node.data.id = node.id
		node.data._node = node
		return node.data

	exists: (model, id, _) =>
		delete @cache[id]
		this.node(id, _)

	find: (model, id, _) =>
		delete @cache[id]
		node = this.node(id, _)
		node.data.id = id if node and node.data
		node.data._node = node

		return @readFromDb(model, node and node.data)

	readFromDb: (model, data) =>
		return data if not data?
		res = {}
		props = this._models[model].properties
		Object.keys(data).forEach((key) ->
			res[key] = if props[key] and props[key].type.name is 'Date' then new Date(data[key]) else data[key])
		res._node = data._node
		return res

	destroy: (model, id, _) =>
		node = this.node(id, _)
		node.delete(_, true)
		delete @cache[id]

	all: (model, filter, _) =>
		if not filter
			nodes = @client.queryNodeIndex(model, 'id:*', _).map_(_, (_, obj) =>
				obj.data.id = obj.id
				obj.data._node = obj
				return @readFromDb(model, obj.data))
		else
			keys = Object.keys(filter.where or {})
			props = Object.keys(this._models[model].properties)
			indeces = intersect keys, props

			if indeces.length > 0
				indexProperty = indeces[0]
				indexValue = filter.where[indeces[0]]

			whereQuery = "query="
			whereArgs = for key, value of filter.where
				"#{key}:#{value}"
			whereQuery = whereQuery + whereArgs.join(' AND ')

			if indexProperty
				nodes = @client.queryIndexedNodes(model, indexProperty, indexValue, whereQuery, _).map_(_, (_, obj) =>
					obj.data.id = obj.id
					obj.data._node = obj
					return @readFromDb(model, obj.data))
			else
				nodes = @client.queryNodeIndex(model, whereQuery, _).map_(_, (_, obj) =>
					obj.data.id = obj.id
					obj.data._node = obj
					return @readFromDb(model, obj.data))

			#nodes.filter(applyFilter(filter)) if filter
			if filter.order
				key = filter.order.split(' ')[0]
				dir = filter.order.split(' ')[1]
				nodes = nodes.sort((a, b) ->
					return a[key] > b[key])
				nodes = nodes.reverse() if dir is 'DESC'
			return nodes

	allNodes: (model, _) =>
		return @client.queryNodeIndex(model, 'id:*', _)

	destroyAll: (model, _) =>
		this.allNodes(model, _).forEach_(_, (_, node) ->
			node.delete(_, true)
		)

	count: (model, _, conds) =>
		collection = this.all(model, {where: conds}, _)
		return collection.length if collection.length
		return 0

	updateAttributes: (model, id, data, _) =>
		data.id = id
		node = this.node(id, _)
		this.save(model, merge(node.data, data), _)

	cleanup = (data) =>
		return null if not data
		res = {};
		Object.keys(data).forEach((key) ->
			v = data[key]
			if v isnt null and not( v.constructor.name is 'Array' and v.length is 0) and typeof v isnt 'undefined'
				res[key] = v)
		return res

	merge = (base, update) =>
		Object.keys(update).forEach((key) ->
			base[key] = update[key])
		return base

	applyFilter = (filter) =>
		test = (example, value) ->
			return value.match(example) if typeof value is 'string' and example and example.constructor.name is 'RegExp'
			return example.toString() is value.toString() if typeof value is 'object' and value.constructor.name is 'Date' and typeof example is 'object' and example.constructor.name is 'Date'
			#not strict equality
			return example == value

		return filter.where if typeof filter.where is 'function'
		keys = Object.keys(filter.where or {})
		return (obj) ->
			pass = true;
			keys.forEach((key) ->
				pass = false if not test(filter.where[key], obj[key]))

	intersect = (a, b) ->
		d = {}
		results = []
		d[i] = true for i in b
		for aItem, j in a
			results.push(a[j]) if d[aItem]
		results
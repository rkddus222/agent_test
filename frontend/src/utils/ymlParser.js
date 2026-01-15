import yaml from 'js-yaml'

/**
 * YML 문자열을 파싱하여 구조화된 데이터로 변환
 */
export function parseYML(ymlContent) {
  try {
    const data = yaml.load(ymlContent)
    return data
  } catch (error) {
    console.error('YML 파싱 오류:', error)
    throw new Error(`YML 파싱 실패: ${error.message}`)
  }
}

/**
 * 구조화된 데이터를 YML 문자열로 변환
 */
export function stringifyYML(data) {
  try {
    return yaml.dump(data, {
      defaultFlowStyle: false,
      allowUnicode: true,
      sortKeys: false,
      indent: 2,
      lineWidth: -1
    })
  } catch (error) {
    console.error('YML 변환 오류:', error)
    throw new Error(`YML 변환 실패: ${error.message}`)
  }
}

/**
 * YML 데이터에서 dimensions와 measures를 추출하여 표 형식 데이터로 변환
 */
export function extractTableData(ymlData) {
  const result = {
    semanticModels: [],
    metrics: []
  }

  if (!ymlData) {
    return result
  }

  // Semantic Models 추출
  if (ymlData.semantic_models) {
    ymlData.semantic_models.forEach((model, modelIndex) => {
      const modelData = {
        name: model.name || '',
        table: model.table || '',
        description: model.description || '',
        label: model.label || '',
        entities: [],
        dimensions: [],
        measures: []
      }

      // Entities 추출
      if (model.entities && Array.isArray(model.entities)) {
        modelData.entities = model.entities.map((entity, index) => ({
          id: `entity-${modelIndex}-${index}`,
          name: entity.name || '',
          type: entity.type || '',
          expr: entity.expr || '',
          label: entity.label || '',
          description: entity.description || '',
          role: entity.role || ''
        }))
      }

      // Dimensions 추출
      if (model.dimensions && Array.isArray(model.dimensions)) {
        modelData.dimensions = model.dimensions.map((dim, index) => ({
          id: `dim-${modelIndex}-${index}`,
          name: dim.name || '',
          type: dim.type || '',
          expr: dim.expr || '',
          label: dim.label || '',
          description: dim.description || '',
          type_params: dim.type_params || null
        }))
      }

      // Measures 추출
      if (model.measures && Array.isArray(model.measures)) {
        modelData.measures = model.measures.map((measure, index) => ({
          id: `measure-${modelIndex}-${index}`,
          name: measure.name || '',
          type: measure.type || '',
          expr: measure.expr || '',
          label: measure.label || '',
          description: measure.description || '',
          agg: measure.agg || ''
        }))
      }

      result.semanticModels.push(modelData)
    })
  }

  // Metrics 추출
  if (ymlData.metrics && Array.isArray(ymlData.metrics)) {
    result.metrics = ymlData.metrics.map((metric, index) => ({
      id: `metric-${index}`,
      name: metric.name || '',
      metric_type: metric.metric_type || '',
      type: metric.type || '',
      expr: metric.expr || '',
      label: metric.label || '',
      description: metric.description || '',
      type_params: metric.type_params || null
    }))
  }

  return result
}

/**
 * 표 형식 데이터를 다시 YML 구조로 변환
 */
export function convertTableDataToYML(tableData, originalYmlData) {
  const ymlData = { ...originalYmlData }

  // Semantic Models 변환
  if (!ymlData.semantic_models) {
    ymlData.semantic_models = []
  }

  // 각 semantic model 업데이트
  tableData.semanticModels.forEach((modelData, modelIndex) => {
    if (!ymlData.semantic_models[modelIndex]) {
      ymlData.semantic_models[modelIndex] = {}
    }

    const model = ymlData.semantic_models[modelIndex]

    // 기본 필드 업데이트
    if (modelData.name) model.name = modelData.name
    if (modelData.table) model.table = modelData.table
    if (modelData.description) model.description = modelData.description
    else if (model.description === '') delete model.description
    if (modelData.label) model.label = modelData.label
    else if (model.label === '') delete model.label

    // Entities 변환
    if (modelData.entities && modelData.entities.length > 0) {
      model.entities = modelData.entities.map(entity => {
        const entityObj = {
          name: entity.name,
          type: entity.type
        }
        if (entity.expr) entityObj.expr = entity.expr
        if (entity.label) entityObj.label = entity.label
        if (entity.description) entityObj.description = entity.description
        if (entity.role) entityObj.role = entity.role
        return entityObj
      })
    } else {
      delete model.entities
    }

    // Dimensions 변환
    if (modelData.dimensions && modelData.dimensions.length > 0) {
      model.dimensions = modelData.dimensions.map(dim => {
        const dimObj = {
          name: dim.name,
          type: dim.type
        }
        if (dim.expr) dimObj.expr = dim.expr
        if (dim.label) dimObj.label = dim.label
        if (dim.description) dimObj.description = dim.description
        if (dim.type_params) dimObj.type_params = dim.type_params
        return dimObj
      })
    } else {
      delete model.dimensions
    }

    // Measures 변환
    if (modelData.measures && modelData.measures.length > 0) {
      model.measures = modelData.measures.map(measure => {
        const measureObj = {
          name: measure.name,
          type: measure.type
        }
        if (measure.expr) measureObj.expr = measure.expr
        if (measure.label) measureObj.label = measure.label
        if (measure.description) measureObj.description = measure.description
        if (measure.agg) measureObj.agg = measure.agg
        return measureObj
      })
    } else {
      delete model.measures
    }
  })

  // Metrics 변환
  if (tableData.metrics && tableData.metrics.length > 0) {
    ymlData.metrics = tableData.metrics.map(metric => {
      const metricObj = {
        name: metric.name,
        metric_type: metric.metric_type
      }
      if (metric.type) metricObj.type = metric.type
      if (metric.expr) metricObj.expr = metric.expr
      if (metric.label) metricObj.label = metric.label
      if (metric.description) metricObj.description = metric.description
      if (metric.type_params) metricObj.type_params = metric.type_params
      return metricObj
    })
  } else {
    // metrics가 빈 배열이면 삭제하지 않고 빈 배열로 유지
    ymlData.metrics = []
  }

  return ymlData
}


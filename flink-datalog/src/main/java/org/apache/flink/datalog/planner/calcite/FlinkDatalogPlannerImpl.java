package org.apache.flink.datalog.planner.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.flink.datalog.parser.ParserManager;
import org.apache.flink.table.calcite.FlinkPlannerImpl;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.calcite.FlinkRelOptClusterFactory;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.catalog.CatalogReader;

import java.util.function.Function;

public class FlinkDatalogPlannerImpl {
	private SqlOperatorTable operatorTable;
	private ImmutableList<RelTraitDef> traitDefs;
	private FrameworkConfig frameworkConfig;
	private Function catalogReaderSupplier;
	private FlinkTypeFactory typeFactory;
	private RelOptPlanner planner;
	private FlinkRelBuilder flinkRelBuilder;
	public FlinkDatalogPlannerImpl(
		FrameworkConfig frameworkConfig,
		Function<Boolean, CalciteCatalogReader> catalogReaderSupplier,
		RelOptPlanner planner,
		FlinkTypeFactory typeFactory,
		FlinkRelBuilder flinkRelBuilder) {
		super();
		this.frameworkConfig = frameworkConfig;
		this.catalogReaderSupplier = catalogReaderSupplier;
		this.typeFactory = typeFactory;
		this.planner = planner;
		this.traitDefs = frameworkConfig.getTraitDefs();
		this.flinkRelBuilder = flinkRelBuilder;
		this.operatorTable = frameworkConfig.getOperatorTable();
	}

	private RexBuilder createRexBuilder() {
		return new RexBuilder(typeFactory);
	}


	//	ImmutableList<RelTraitDef<? extends RelTrait>> traitDefs = frameworkConfig.getTraitDefs();
//	val parserConfig: SqlParser.Config = config.getParserConfig
//	val convertletTable: SqlRexConvertletTable = config.getConvertletTable
//	val sqlToRelConverterConfig: SqlToRelConverter.Config = config.getSqlToRelConverterConfig
//
//	var validator: FlinkCalciteSqlValidator = _
//	var root: RelRoot = _
//
	private void ready() {
		if (this.traitDefs != null) {
			this.planner.clearRelTraitDefs();
			for (RelTraitDef traitDef : this.traitDefs) {
				this.planner.addRelTraitDef(traitDef);
			}
		}
	}

//	def getCompletionHints(sql: String, cursor: Int): Array[String] = {
//		val advisorValidator = new SqlAdvisorValidator(
//			operatorTable,
//			catalogReaderSupplier.apply(true), // ignore cases for lenient completion
//			typeFactory,
//			config.getParserConfig.conformance())
//		val advisor = new SqlAdvisor(advisorValidator, config.getParserConfig)
//		val replaced = Array[String](null)
//		val hints = advisor.getCompletionHints(sql, cursor, replaced)
//			.map(item => item.toIdentifier.toString)
//		hints.toArray
//	}
//
//	/**
//	 * Get the [[FlinkCalciteSqlValidator]] instance from this planner, create a new instance
//	 * if current validator has not been initialized, or returns the validator
//	 * instance directly.
//	 *
//	 * <p>The validator instance creation is not thread safe.
//	 *
//	 * @return a new validator instance or current existed one
//	 */
//	def getOrCreateSqlValidator(): FlinkCalciteSqlValidator = {
//		if (validator == null) {
//			val catalogReader = catalogReaderSupplier.apply(false)
//			validator = new FlinkCalciteSqlValidator(
//				operatorTable,
//				catalogReader,
//				typeFactory)
//			validator.setIdentifierExpansion(true)
//			validator.setDefaultNullCollation(FlinkPlannerImpl.defaultNullCollation)
//			// Disable implicit type coercion for now.
//			validator.setEnableTypeCoercion(false)
//		}
//		validator
//	}
//
	public RelNode parse(String inputProgram, String query) {
		try {
//			ready();
//			RexBuilder rexBuilder = createRexBuilder();
//			RelOptCluster cluster = FlinkRelOptClusterFactory.create(planner, rexBuilder);
//			CatalogReader catalogReader = (CatalogReader) catalogReaderSupplier.apply(false);
			ParserManager parserManager = new ParserManager(this.flinkRelBuilder);
			return parserManager.parse(inputProgram, query);

		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

//	def validate(sqlNode: SqlNode): SqlNode = {
//		val catalogReader = catalogReaderSupplier.apply(false)
//		// do pre-validate rewrite.
//		sqlNode.accept(new PreValidateReWriter(catalogReader, typeFactory))
//		// do extended validation.
//		sqlNode match {
//			case node: ExtendedSqlNode =>
//				node.validate()
//			case _ =>
//		}
//		// no need to validate row type for DDL and insert nodes.
//		if (sqlNode.getKind.belongsTo(SqlKind.DDL)
//			|| sqlNode.getKind == SqlKind.INSERT) {
//			return sqlNode
//		}
//		try {
//			getOrCreateSqlValidator().validate(sqlNode)
//		} catch {
//			case e: RuntimeException =>
//				throw new ValidationException(s"SQL validation failed. ${e.getMessage}", e)
//		}
//	}
//
//	def rel(validatedSqlNode: SqlNode): RelRoot = {
//		try {
//			assert(validatedSqlNode != null)
//			val sqlToRelConverter: SqlToRelConverter = new SqlToRelConverter(
//				new ViewExpanderImpl,
//				getOrCreateSqlValidator(),
//				catalogReaderSupplier.apply(false),
//				cluster,
//				convertletTable,
//				sqlToRelConverterConfig)
//			root = sqlToRelConverter.convertQuery(validatedSqlNode, false, true)
//			// we disable automatic flattening in order to let composite types pass without modification
//			// we might enable it again once Calcite has better support for structured types
//			// root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true))
//
//			// TableEnvironment.optimize will execute the following
//			// root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel))
//			// convert time indicators
//			// root = root.withRel(RelTimeIndicatorConverter.convert(root.rel, rexBuilder))
//			root
//		} catch {
//			case e: RelConversionException => throw new TableException(e.getMessage)
//		}
//	}
//
//	/** Implements [[org.apache.calcite.plan.RelOptTable.ViewExpander]]
//	 * interface for [[org.apache.calcite.tools.Planner]]. */
//	class ViewExpanderImpl extends ViewExpander {
//
//		override def expandView(
//			rowType: RelDataType,
//			queryString: String,
//			schemaPath: util.List[String],
//			viewPath: util.List[String]): RelRoot = {
//
//			val parser: SqlParser = SqlParser.create(queryString, parserConfig)
//			var sqlNode: SqlNode = null
//			try {
//				sqlNode = parser.parseQuery
//			}
//			catch {
//				case e: CSqlParseException =>
//					throw new SqlParserException(s"SQL parse failed. ${e.getMessage}", e)
//			}
//			val catalogReader: CalciteCatalogReader = catalogReaderSupplier.apply(false)
//				.withSchemaPath(schemaPath)
//			val validator: SqlValidator =
//				new FlinkCalciteSqlValidator(operatorTable, catalogReader, typeFactory)
//			validator.setIdentifierExpansion(true)
//			val validatedSqlNode: SqlNode = validator.validate(sqlNode)
//			val sqlToRelConverter: SqlToRelConverter = new SqlToRelConverter(
//				new ViewExpanderImpl,
//				validator,
//				catalogReader,
//				cluster,
//				convertletTable,
//				sqlToRelConverterConfig)
//			root = sqlToRelConverter.convertQuery(validatedSqlNode, true, false)
//			root = root.withRel(sqlToRelConverter.flattenTypes(root.project(), true))
//			root = root.withRel(RelDecorrelator.decorrelateQuery(root.project()))
//			FlinkPlannerImpl.this.root
//		}
//	}

}

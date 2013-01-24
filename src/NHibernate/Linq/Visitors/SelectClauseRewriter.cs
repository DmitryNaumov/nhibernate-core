using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Iesi.Collections.Generic;
using Remotion.Linq.Clauses.Expressions;

namespace NHibernate.Linq.Visitors
{
	using Remotion.Linq.Parsing;

	public sealed class SelectClauseRewriter
	{
		class QuerySourceVisitor : ExpressionTreeVisitor
		{
			private List<QuerySourceReferenceExpression> _querySourceReferenceExpressions;

			public List<QuerySourceReferenceExpression> QuerySourceReferenceExpressions
			{
				get { return _querySourceReferenceExpressions; }
			}


			protected override Expression VisitMemberExpression(MemberExpression expression)
			{
				var querySourceReferenceExpression = expression.Expression as QuerySourceReferenceExpression;
				if (querySourceReferenceExpression != null)
				{
					System.Type memberType = null;
					
					var propertyInfo = expression.Member as PropertyInfo;
					if (propertyInfo != null && IsCollectionType(propertyInfo.PropertyType))
					{
						memberType = propertyInfo.PropertyType;
					}

					var fieldInfo = expression.Member as FieldInfo;
					if (fieldInfo != null && IsCollectionType(fieldInfo.FieldType))
					{
						memberType = fieldInfo.FieldType;
					}

					if (memberType != null && IsCollectionType(memberType))
					{
						var list = _querySourceReferenceExpressions ??
								   (_querySourceReferenceExpressions = new List<QuerySourceReferenceExpression>());

						list.Add(querySourceReferenceExpression);
					}
				}

				return base.VisitMemberExpression(expression);
			}

			protected override Expression VisitUnknownExpression(Expression expression)
			{
				return expression;
			}

			protected override Expression VisitUnknownNonExtensionExpression(Expression expression)
			{
				return expression;
			}

			private bool IsCollectionType(System.Type type)
			{
				return typeof(IEnumerable).IsAssignableFrom(type) && type != typeof(string);
			}
		}

		private static readonly MethodInfo CastMethod = typeof(Enumerable).GetMethod("Cast", BindingFlags.Public | BindingFlags.Static);
		private static readonly MethodInfo GroupByMethod = ReflectionHelper.GetMethodDefinition(() => Enumerable.GroupBy(Enumerable.Empty<int>(), i => i, i => i));
		private static readonly MethodInfo ToArrayMethod = ReflectionHelper.GetMethodDefinition(() => Enumerable.ToArray(Enumerable.Empty<int>()));
		private static readonly MethodInfo SelectMethod = ReflectionHelper.GetMethodDefinition(() => Enumerable.Select(Enumerable.Empty<int>(), i => i));

		private readonly ParameterExpression _inputParameter;

		private Expression _resultExpression;
		private List<int> _indicies = new List<int>();

		public SelectClauseRewriter(ParameterExpression inputParameter)
		{
			_inputParameter = inputParameter;
		}

		public Expression ListTransformer { get; private set; }

		public Expression PreProcess(Expression expression)
		{
			_resultExpression = expression;

			switch (expression.NodeType)
			{
				case ExpressionType.MemberAccess:
					return OnMemberAccess((MemberExpression)expression);
				case ExpressionType.New:
					return OnNewExpression((NewExpression)expression);
				case ExpressionType.MemberInit:
					return OnMemberInitExpression((MemberInitExpression)expression);
			}

			return expression;
		}

		public Expression PostProcess(Expression expression)
		{
			if (_indicies.Count == 0)
			{
				return expression;
			}

			return PostProcess(expression as NewExpression) ?? expression;
		}

		private Expression PostProcess(NewExpression expression)
		{
			if (expression == null)
			{
				return null;
			}

			var arguments = expression.Arguments.ToArray();

			_indicies.ForEach(index =>
			{
				//arguments[index] = Expression.Convert(((UnaryExpression) arguments[index]).Operand, arguments[index].Type.GetGenericArguments()[0]);
				arguments[index] =
					Expression.Convert(Expression.ArrayIndex(_inputParameter, Expression.Constant(index, typeof (int))),
					                   arguments[index].Type.GetGenericArguments()[0]);
			});

			var typeArguments = arguments.Select(arg => arg.Type).ToArray();
			var type = expression.Type.GetGenericTypeDefinition().MakeGenericType(typeArguments);


			expression = Expression.New(type.GetConstructor(typeArguments), arguments);

			ListTransformer = CreateListTransformer(expression);

			return expression;
		}

		private Expression OnMemberAccess(MemberExpression expression)
		{
			return TryConvertProjection(expression) ?? expression;
		}

		private Expression OnNewExpression(NewExpression expression)
		{
			return TryConvertProjection(expression.Arguments.ToArray()) ?? expression;
		}

		private Expression OnMemberInitExpression(MemberInitExpression expression)
		{
			var arguments = new List<Expression>();
			arguments.AddRange(expression.NewExpression.Arguments);
			arguments.AddRange(expression.Bindings.Cast<MemberAssignment>().Select(x => x.Expression));

			return TryConvertProjection(arguments.ToArray()) ?? expression;
		}

		private Expression TryConvertProjection(params Expression[] expressions)
		{
			var indicies = new List<int>();
			var list = new List<QuerySourceReferenceExpression>();

			int n = 0;
			expressions.ForEach(expression =>
			{
				var visitor = new QuerySourceVisitor();
				visitor.VisitExpression(expression);

				if (visitor.QuerySourceReferenceExpressions != null)
				{
					list.AddRange(visitor.QuerySourceReferenceExpressions);
					indicies.Add(n + 1);
				}

				++n;
			});

			if (list.Count == 0 || list.Distinct().Count() > 1)
			{
				// different query sources are referenced, not supported
				return null;
			}

			var querySourceExpression = list[0];

			var typeArgs = new List<System.Type>();
			typeArgs.Add(querySourceExpression.ReferencedQuerySource.ItemType);
			typeArgs.AddRange(expressions.Select(expr => expr.Type));

			var arguments = new List<Expression>();
			
			// Add collection owner to projection
			arguments.Add(querySourceExpression);
			arguments.AddRange(expressions);

			var projection = Expression.New(GetTupleConstructor(typeArgs.ToArray()), arguments);

			_indicies = indicies;

			return projection;
		}

		private Expression CreateListTransformer(NewExpression expression)
		{
			var itemParameter = Expression.Parameter(expression.Type, "item");
			var keySelector = Expression.Lambda(Expression.New(expression.Constructor,
											 expression.Arguments.Select(
												 (arg, n) =>
												 _indicies.Contains(n)
													 ? (Expression)Expression.Constant(null, arg.Type)
													 : Expression.Property(itemParameter, GetTuplePropertyByIndex(expression.Type, n)))), itemParameter);

			var valueSelector =
				Expression.Lambda(Expression.Property(itemParameter, GetTuplePropertyByIndex(expression.Type, _indicies[0])), itemParameter);

			return CreateListTransformer2(expression.Type, keySelector, valueSelector);
		}

		private ConstructorInfo GetTupleConstructor(params System.Type[] parameters)
		{
			switch (parameters.Length)
			{
				case 2:
					return typeof(Tuple<,>).MakeGenericType(parameters).GetConstructor(parameters);
				case 3:
					return typeof(Tuple<,,>).MakeGenericType(parameters).GetConstructor(parameters);
				case 4:
					return typeof(Tuple<,,,>).MakeGenericType(parameters).GetConstructor(parameters);
				case 5:
					return typeof(Tuple<,,,,>).MakeGenericType(parameters).GetConstructor(parameters);
			}

			throw new ArgumentException("Number of parameters should be greater than 1 and less than 6", "parameters");
		}

		private PropertyInfo GetTuplePropertyByIndex(System.Type type, int index)
		{
			switch (index)
			{
				case 0:
					return type.GetProperty("First");
				case 1:
					return type.GetProperty("Second");
				case 2:
					return type.GetProperty("Third");
				case 3:
					return type.GetProperty("Forth");
				case 4:
					return type.GetProperty("Fifth");
			}

			throw new ArgumentOutOfRangeException("index");
		}

		private Expression CreateListTransformer2(System.Type elementType, LambdaExpression keySelector, LambdaExpression valueSelector)
		{
			var cast = Expression.Call(null, CastMethod.MakeGenericMethod(elementType), _inputParameter);

			var tKey = keySelector.Body.Type;
			var tValue = valueSelector.Body.Type;
			var groupBy = Expression.Call(null, GroupByMethod.MakeGenericMethod(elementType, tKey, tValue), cast, keySelector, valueSelector);

			var groupType = typeof(IGrouping<,>).MakeGenericType(tKey, tValue);
			var groupParam = Expression.Parameter(groupType, "g");

			var newCollection = GetResultSelector(groupParam);

			var select = Expression.Call(null, SelectMethod.MakeGenericMethod(groupType, newCollection.Body.Type), groupBy, newCollection);

			var result = Expression.Call(null, ToArrayMethod.MakeGenericMethod(newCollection.Body.Type), select);

			return result;
		}

		private LambdaExpression GetResultSelector(ParameterExpression inputParameter)
		{
			// .GroupBy(...).Select(g => new {g.Key.First, g.Key.Second, ..., new HashedSet<>(g.ToArray())})

			var values = ExtractValuesFromGroup(inputParameter);

			var resultType = _resultExpression.Type;
			if (_resultExpression.NodeType == ExpressionType.MemberAccess)
			{
				return Expression.Lambda(values[0], inputParameter);
			}
			else if (_resultExpression.NodeType == ExpressionType.MemberInit)
			{
				var resultExpression = (MemberInitExpression) _resultExpression;

				// TODO: constructor may also have some arguments
				int n = 0;
				var bindings = resultExpression.Bindings.Select(mb => Expression.Bind(mb.Member, values[n++])).ToArray();
				var memberInitExpression = Expression.MemberInit(resultExpression.NewExpression, bindings);

				return Expression.Lambda(memberInitExpression, inputParameter);
			}
			else if (_resultExpression.NodeType == ExpressionType.New)
			{
				var ctor = resultType.GetConstructor(values.Select(x => x.Type).ToArray());
				var newCollection = Expression.Lambda(Expression.New(ctor, values), inputParameter);
				return newCollection;
			}

			throw new NotImplementedException();
		}

		private Expression[] ExtractValuesFromGroup(ParameterExpression inputParameter)
		{
			if (!inputParameter.Type.IsGenericType || inputParameter.Type.GetGenericTypeDefinition() != typeof(IGrouping<,>))
			{
				throw new InvalidProgramException();
			}

			var keyPropertyExpression = Expression.Property(inputParameter, inputParameter.Type.GetProperty("Key"));

			var expressions = keyPropertyExpression.Type.GetProperties()
							.Select((_, n) => Expression.Property(keyPropertyExpression, GetTuplePropertyByIndex(keyPropertyExpression.Type, n)))
							.Cast<Expression>()
							.ToArray();

			var groupToArrayExpression = GetGroupToArrayExpression(inputParameter);
			_indicies.ForEach(index =>
			{
				// TODO: add support for .ToList()
				expressions[index] = groupToArrayExpression;
			});

			return expressions.Skip(1).ToArray();
		}

		private Expression GetGroupToArrayExpression(ParameterExpression inputParameter)
		{
			// TODO: what if result collection is non-generic?
			var itemType = inputParameter.Type.GetGenericArguments()[1];

			var collectionType = GetConcreteCollectionType(itemType);
			// TODO: bind to right constructor
			var ctor = collectionType.GetConstructors().First(ci => ci.GetParameters().Length > 0);

			var toArray = Expression.Call(null, ToArrayMethod.MakeGenericMethod(itemType), inputParameter);
			return Expression.New(ctor, toArray);
		}

		private System.Type GetConcreteCollectionType(System.Type itemType)
		{
			// TODO: detect concrete collection type
			return typeof (HashedSet<>).MakeGenericType(itemType);
		}
	}
}
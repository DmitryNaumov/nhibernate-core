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
	public sealed class SelectClauseRewriter
	{
		private static readonly MethodInfo CastMethod = typeof(Enumerable).GetMethod("Cast", BindingFlags.Public | BindingFlags.Static);
		private static readonly MethodInfo GroupByMethod = ReflectionHelper.GetMethodDefinition(() => Enumerable.GroupBy(Enumerable.Empty<int>(), i => i, i => i));
		private static readonly MethodInfo ToArrayMethod = ReflectionHelper.GetMethodDefinition(() => Enumerable.ToArray(Enumerable.Empty<int>()));
		private static readonly MethodInfo SelectMethod = ReflectionHelper.GetMethodDefinition(() => Enumerable.Select(Enumerable.Empty<int>(), i => i));

		private readonly ParameterExpression _inputParameter;

		private Expression _resultExpression;

		private bool _collectionOwnerWasInjected;
		private int _index = -1;

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
			}

			return expression;
		}

		public Expression PostProcess(Expression expression)
		{
			var projection = expression as NewExpression;
			if (projection == null)
			{
				return expression;
			}

			return PostProcess(projection);
		}

		private Expression PostProcess(NewExpression expression)
		{
			if (_index == -1)
			{
				return expression;
			}

			var arguments = expression.Arguments.ToArray();

			// TODO: index of collection
			arguments[_index] = Expression.Convert(((UnaryExpression)arguments[_index]).Operand,
			                                  ((UnaryExpression)arguments[_index]).Type.GetGenericArguments()[0]);

			var typeArguments = arguments.Select(arg => arg.Type).ToArray();
			var type = expression.Type.GetGenericTypeDefinition().MakeGenericType(typeArguments);

			expression = Expression.New(type.GetConstructor(typeArguments), arguments);

			ListTransformer = CreateListTransformer(expression);

			return expression;
		}

		private Expression OnMemberAccess(MemberExpression expression)
		{
			return TryInjectCollectionOwnerIntoProjection(expression) ?? expression;
		}

		private Expression OnNewExpression(NewExpression expression)
		{
			return TryInjectCollectionOwnerIntoProjection(expression.Arguments.ToArray()) ?? expression;
		}

		private Expression TryInjectCollectionOwnerIntoProjection(params Expression[] expressions)
		{
			// TODO: will throw if multiple collections in projection
			var expression =
				expressions.OfType<MemberExpression>().SingleOrDefault(expr => GetCollectionQuerySource(expr) != null);

			if (expression == null)
			{
				return null;
			}

			_index = Array.IndexOf(expressions, expression);

			var querySource = GetCollectionQuerySource(expression);

			NewExpression projection = null;
			if (Array.IndexOf(expressions, querySource) == -1)
			{
				// let's include collection owner into expression

				var typeArgs = new List<System.Type>() { expression.Member.DeclaringType };
				typeArgs.AddRange(expressions.Select(expr => expr.Type));

				var ctor = GetTupleConstructor(typeArgs.ToArray());

				var ctorArguments = new List<Expression>() { querySource };
				ctorArguments.AddRange(expressions);

				projection = Expression.New(ctor, ctorArguments);

				_collectionOwnerWasInjected = true;
				_index++;
			}
			else
			{
				// let's use Tuple<> instead of anonymous type

				var typeArgs = expressions.Select(expr => expr.Type).ToArray();
				var ctor = GetTupleConstructor(typeArgs);
				projection = Expression.New(ctor, expressions);
			}

			return projection;
		}

		private Expression CreateListTransformer(NewExpression expression)
		{
			if (expression == null || _index == -1)
			{
				return null;
			}

			var itemParameter = Expression.Parameter(expression.Type, "item");

			LambdaExpression keySelector = null;
			if (expression.Arguments.Count == 2)
			{
				// TODO: collection owner may be not the first
				keySelector = Expression.Lambda(Expression.MakeMemberAccess(itemParameter, expression.Type.GetProperty("First")), itemParameter);
			}
			else
			{
				// TODO:
				throw new NotImplementedException();
			}

			LambdaExpression valueSelector = null;
			if (expression.Arguments.Count == 2)
			{
				valueSelector = Expression.Lambda(Expression.MakeMemberAccess(itemParameter, expression.Type.GetProperty("Second")), itemParameter);
			}
			else
			{
				// TODO:
				throw new NotImplementedException();
			}

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
			}

			throw new ArgumentException("Number of parameters should be greater than 1 and less than 5", "parameters");
		}

		private Expression CreateListTransformer2(System.Type elementType, LambdaExpression keySelector, LambdaExpression valueSelector)
		{
			var cast = Expression.Call(null, CastMethod.MakeGenericMethod(elementType), _inputParameter);

			var tKey = keySelector.Body.Type;
			var tValue = valueSelector.Body.Type;
			var groupBy = Expression.Call(null, GroupByMethod.MakeGenericMethod(elementType, tKey, tValue), cast, keySelector, valueSelector);

			var groupType = typeof(IGrouping<,>).MakeGenericType(tKey, tValue);
			var groupParam = Expression.Parameter(groupType, "g");

			var newCollection = GetResultSelector(tValue, groupParam);

			var select = Expression.Call(null, SelectMethod.MakeGenericMethod(groupType, newCollection.Body.Type), groupBy, newCollection);

			var result = Expression.Call(null, ToArrayMethod.MakeGenericMethod(newCollection.Body.Type), select);

			return result;
		}

		private LambdaExpression GetResultSelector(System.Type itemType, ParameterExpression inputParameter)
		{
			// .GroupBy(...).Select(g => new {g.Key.First, g.Key.Second, ..., new HashedSet<>(g.ToArray())})

			var values = ExtractValuesFromGroup(inputParameter);

			var resultType = _resultExpression.Type;
			if (IsCollectionType(resultType))
			{
				return Expression.Lambda(values[0], inputParameter);
			}

			// TODO: bind to right constructor
			var ctor = resultType.GetConstructors().First(ci => ci.GetParameters().Length > 0);

			var newCollection = Expression.Lambda(Expression.New(ctor, values), inputParameter);

			return newCollection;
		}

		private Expression[] ExtractValuesFromGroup(ParameterExpression inputParameter)
		{
			if (!inputParameter.Type.IsGenericType || inputParameter.Type.GetGenericTypeDefinition() != typeof(IGrouping<,>))
			{
				throw new InvalidProgramException();
			}

			var keyProperty = Expression.Property(inputParameter, inputParameter.Type.GetProperty("Key"));

			var result = new List<Expression>();

			var tKey = inputParameter.Type.GetGenericArguments()[0];
			var tValue = inputParameter.Type.GetGenericArguments()[1];

			if (typeof(ITuple).IsAssignableFrom(tKey))
			{
				result.AddRange(ExtractValuesFromTuple(keyProperty));
			}
			else
			{
				if (_collectionOwnerWasInjected)
				{
					// ignore keyProperty
					return new [] { CreateCollection(tValue, inputParameter) };
				}

				return new [] { keyProperty, CreateCollection(tValue, inputParameter) };
			}

			return null;
		}

		private Expression[] ExtractValuesFromTuple(MemberExpression expression)
		{
			var type = expression.Type;

			return null;
		}

		private Expression CreateCollection(System.Type itemType, ParameterExpression inputParameter)
		{
			var collectionType = GetConcreteCollectionType(itemType);
			var ctor = collectionType.GetConstructors().First(ci => ci.GetParameters().Length > 0);

			var toArray = Expression.Call(null, ToArrayMethod.MakeGenericMethod(itemType), inputParameter);
			return Expression.New(ctor, toArray);
		}

		private QuerySourceReferenceExpression GetCollectionQuerySource(MemberExpression expression)
		{
			if (IsCollectionType(expression.Type))
			{
				return expression.Expression as QuerySourceReferenceExpression;
			}

			return null;
		}

		private System.Type GetConcreteCollectionType(System.Type itemType)
		{
			// TODO: detect concrete collection type
			return typeof (HashedSet<>).MakeGenericType(itemType);
		}

		private bool IsCollectionType(System.Type type)
		{
			return typeof (IEnumerable).IsAssignableFrom(type) && type != typeof (string);
		}
	}
}
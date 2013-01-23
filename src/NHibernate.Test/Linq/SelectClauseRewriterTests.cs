// NOTE: intentionally changed namespace to skip database setup when running these tests
namespace NHibernate.UnitTest.Linq
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using System.Reflection;
	using Iesi.Collections.Generic;
	using NUnit.Framework;
	using System.Linq.Expressions;
	using NHibernate.Linq.Visitors;
	using Remotion.Linq.Clauses;
	using Remotion.Linq.Clauses.Expressions;

	[TestFixture]
	public class SelectClauseRewriterTests
	{
		class Order
		{
		}

		class Customer
		{
			public static readonly PropertyInfo OrderSetProperty = typeof (Customer).GetProperty("OrderSet");
			public static readonly PropertyInfo OrderListProperty = typeof(Customer).GetProperty("OrderList");

			public ISet<Order> OrderSet { get; set; }
			public IList<Order> OrderList { get; set; }

			public int Age { get; private set; }

			public bool VIP { get; private set; }
		}

		[TestFixture]
		public class PreProcessTests
		{
			class ExpandedWrapper<T1, T2>
			{
				public T1 A { get; set; }
				public T2 B { get; set; }
			}

			private SelectClauseRewriter _cut;
			private IQuerySource _querySource;

			[SetUp]
			public void SetUp()
			{
				var inputParameter = Expression.Parameter(typeof (object[]), "input");
				_cut = new SelectClauseRewriter(inputParameter);
				_querySource = new MainFromClause("customer", typeof (Customer), Expression.Parameter(typeof (Customer), "list"));
			}

			[Test]
			public void AddsCollectionOwner()
			{
				var expression = Expression.Property(new QuerySourceReferenceExpression(_querySource), Customer.OrderSetProperty);
				var result = _cut.PreProcess(expression);

				Assert.AreEqual(typeof (NHibernate.Linq.Tuple<Customer, ISet<Order>>), result.Type);
			}

			[Test]
			public void AddsCollectionOwner2()
			{
				var customer = new Customer();
				var projection = new {customer.OrderSet};

				var expression = Expression.New(GetConstructor(projection),
				                                Expression.Property(new QuerySourceReferenceExpression(_querySource),
				                                                    Customer.OrderSetProperty));

				var result = _cut.PreProcess(expression);

				Assert.AreEqual(typeof (NHibernate.Linq.Tuple<Customer, ISet<Order>>), result.Type);
			}

			[Test]
			public void DoesNotAddCollectionOwnerWhenPresentInAnonymousProjection()
			{
				var customer = new Customer();
				var projection = new {customer, customer.OrderSet};

				var querySourceExpression = new QuerySourceReferenceExpression(_querySource);
				var expression = Expression.New(GetConstructor(projection),
				                                querySourceExpression,
				                                Expression.Property(querySourceExpression, Customer.OrderSetProperty));

				var result = _cut.PreProcess(expression);

				Assert.AreEqual(typeof (NHibernate.Linq.Tuple<Customer, ISet<Order>>), result.Type);
			}

			[Test]
			public void DoesNotAddCollectionOwnerWhenPresentInKnownType()
			{
				var customer = new Customer();
				var projection = new ExpandedWrapper<Customer, ISet<Order>> { A = customer, B = customer.OrderSet };

				var querySourceExpression = new QuerySourceReferenceExpression(_querySource);
				var expression = Expression.MemberInit(
									Expression.New(projection.GetType().GetConstructors()[0]),
												new MemberBinding[] 
												{
													Expression.Bind(projection.GetType().GetProperty("A"), querySourceExpression),
													Expression.Bind(projection.GetType().GetProperty("B"), Expression.Property(querySourceExpression, Customer.OrderSetProperty)),
												});

				var result = _cut.PreProcess(expression);

				Assert.AreEqual(typeof(NHibernate.Linq.Tuple<Customer, ISet<Order>>), result.Type);
			}

			[Test]
			public void RespectsParameterOrderWhenInjects()
			{
				var customer = new Customer();
				var projection = new {customer.Age, customer.OrderSet, customer.VIP};

				var querySourceExpression = new QuerySourceReferenceExpression(_querySource);
				var expression = Expression.New(GetConstructor(projection),
				                                Expression.Constant(1, typeof (int)),
				                                Expression.Property(querySourceExpression, Customer.OrderSetProperty),
				                                Expression.Constant(true, typeof (bool)));

				var result = _cut.PreProcess(expression);

				Assert.AreEqual(typeof (NHibernate.Linq.Tuple<Customer, int, ISet<Order>, bool>), result.Type);
			}

			[Test]
			public void RespectsParameterOrderWhenDoesNotInject()
			{
				var customer = new Customer();
				var projection = new {customer.Age, customer, customer.OrderSet, customer.VIP};

				var querySourceExpression = new QuerySourceReferenceExpression(_querySource);
				var expression = Expression.New(GetConstructor(projection),
				                                Expression.Constant(1, typeof (int)),
				                                querySourceExpression,
				                                Expression.Property(querySourceExpression, Customer.OrderSetProperty),
				                                Expression.Constant(true, typeof (bool)));

				var result = _cut.PreProcess(expression);

				Assert.AreEqual(typeof (NHibernate.Linq.Tuple<int, Customer, ISet<Order>, bool>), result.Type);
			}

			private ConstructorInfo GetConstructor(object obj)
			{
				var type = obj.GetType();
				return type.GetConstructor(type.GetProperties().Select(pi => pi.PropertyType).ToArray());
			}
		}
	}
}

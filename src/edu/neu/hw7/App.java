package edu.neu.hw7;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Scanner;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.sql.PreparedStatement;

public class App {
	static final String className = "org.apache.phoenix.jdbc.PhoenixDriver";
	static final String connectionName = "jdbc:phoenix:localhost";

	public static void main(String[] args)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {

		Class.forName(className).newInstance();
		java.sql.Connection con = DriverManager.getConnection(connectionName);
		// Users
		try {

			java.sql.Statement stmt = null;
			java.sql.ResultSet rset = null;
			// create table
			stmt = con.createStatement();
			stmt.executeUpdate("drop table IF EXISTS user");
			stmt.executeUpdate(
					"create table user (userId varchar not null primary key, location varchar, age varchar)");
			con.commit();
			System.out.println("user table created!");
			// Insert record
			String userFile = "doc/BX-Users.csv";
			File filem = new File(new File(userFile).getAbsolutePath());
			try {
				BufferedReader br = new BufferedReader(new FileReader(filem));
				br.readLine(); // skip first line
				String line = null;

				while ((line = br.readLine()) != null) {
					String data = line;
					// data = data.replaceAll(",", ",,");
					String[] value = data.split(";");
					String userId = "";
					String location = "";
					String age = "";
					if (value.length == 3) {
						// movieId = Integer.parseInt(value[0]);
						userId = value[0].replaceAll("^\"|\"$", "");
						location = value[1].replaceAll("^\"|\"$", "");
						age = value[2].replaceAll("^\"|\"$", "");
						String query = "upsert into user values ('" + userId + "','" + location + "','" + age + "')";
						// System.out.println("User Query Insert========"+
						// query);
						stmt.executeUpdate(query);
						con.commit();
					} else if (value.length == 2) {
						userId = value[0].replaceAll("^\"|\"$", "");
						location = value[1].replaceAll("^\"|\"$", "");
						stmt.executeUpdate("upsert into user values ('" + userId + "','" + location + "','')");
						con.commit();
					} else {
						userId = value[0].replaceAll("^\"|\"$", "");
						stmt.executeUpdate("upsert into user values ('" + userId + "','','')");
						con.commit();
					}
					// con.commit();
				}
				// System.out.println("===========get Info========");
				// PreparedStatement statement = con.prepareStatement("select *
				// from user");
				// rset = statement.executeQuery();
				// while (rset.next()) {
				// System.out.println(rset.getString("location"));
				// }
				// statement.close();
				con.close();
				System.out.println("===All records inserted to user table!===");

			} catch (Exception e) {
				e.printStackTrace();
			}
			/*
			 * PreparedStatement statement = con.prepareStatement(
			 * "select * from test"); rset = statement.executeQuery(); while
			 * (rset.next()) { System.out.println(rset.getString("mycolumn")); }
			 * statement.close(); con.close();
			 */
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Books
		try {

			java.sql.Statement stmt = null;
			java.sql.ResultSet rset = null;
			// create table"
			stmt = con.createStatement();
			stmt.executeUpdate("drop table IF EXISTS books");
			stmt.executeUpdate("create table books(ISBN varchar not null primary key,"
					+ "title varchar, author varchar," + "yearOfPublication varchar," + "publisher varchar,"
					+ " imageURLS varchar, imageURLM varchar, imageURLL varchar)");
			con.commit();
			System.out.println("books table created!");
			// Insert record
			String bookFile = "doc/BX-Books.csv";
			File filem = new File(new File(bookFile).getAbsolutePath());
			try {
				BufferedReader br = new BufferedReader(new FileReader(filem));
				br.readLine(); // skip first line
				String line = null;

				while ((line = br.readLine()) != null) {
					String data = line;
					// data = data.replaceAll(",", ",,");

					data = data.replaceAll("'", "''");

					// System.out.println("Data =====" + data);
					String[] value = data.split(";");
					String isbn = "";
					String title = "";
					String author = "";
					String yop = "";
					String publisher = "";
					String ius = "";
					String ium = "";
					String iul = "";
					if (value.length == 8) {
						// movieId = Integer.parseInt(value[0]);
						isbn = value[0].replaceAll("^\"|\"$", "");
						title = value[1].replaceAll("^\"|\"$", "");
						author = value[2].replaceAll("^\"|\"$", "");
						yop = value[3].replaceAll("^\"|\"$", "");
						publisher = value[4].replaceAll("^\"|\"$", "");
						ius = value[5].replaceAll("^\"|\"$", "");
						ium = value[6].replaceAll("^\"|\"$", "");
						iul = value[7].replaceAll("^\"|\"$", "");
						String query = "upsert into books values ('" + isbn + "','" + title + "','" + author + "','"
								+ yop + "','" + publisher + "','" + ius + "','" + ium + "','" + iul + "')";
						// System.out.println("the query is ======"+query);
						stmt.executeUpdate(query);
						con.commit();
					} else if (value.length == 7) {
						isbn = value[0].replaceAll("^\"|\"$", "");
						title = value[1].replaceAll("^\"|\"$", "");
						author = value[2].replaceAll("^\"|\"$", "");
						yop = value[3].replaceAll("^\"|\"$", "");
						publisher = value[4].replaceAll("^\"|\"$", "");
						ius = value[5].replaceAll("^\"|\"$", "");
						ium = value[6].replaceAll("^\"|\"$", "");
						String query = "upsert into books values ('" + isbn + "','" + title + "','" + author + "','"
								+ yop + "','" + publisher + "','" + ius + "','" + ium + "','')";
						// System.out.println("the query is ======"+query);
						stmt.executeUpdate(query);
						con.commit();
					} else if (value.length == 6) {
						isbn = value[0].replaceAll("^\"|\"$", "");
						title = value[1].replaceAll("^\"|\"$", "");
						author = value[2].replaceAll("^\"|\"$", "");
						yop = value[3].replaceAll("^\"|\"$", "");
						publisher = value[4].replaceAll("^\"|\"$", "");
						ius = value[5].replaceAll("^\"|\"$", "");
						String query = "upsert into books values ('" + isbn + "','" + title + "','" + author + "','"
								+ yop + "','" + publisher + "','" + ius + "','','')";
						// System.out.println("the query is ======"+query);
						stmt.executeUpdate(query);
						con.commit();
					} else if (value.length == 5) {
						isbn = value[0].replaceAll("^\"|\"$", "");
						title = value[1].replaceAll("^\"|\"$", "");
						author = value[2].replaceAll("^\"|\"$", "");
						yop = value[3].replaceAll("^\"|\"$", "");
						publisher = value[4].replaceAll("^\"|\"$", "");
						String query = "upsert into books values ('" + isbn + "','" + title + "','" + author + "','"
								+ yop + "','" + publisher + "','','','')";
						// System.out.println("the query is ======"+query);
						stmt.executeUpdate(query);
						con.commit();
					} else if (value.length == 4) {
						isbn = value[0].replaceAll("^\"|\"$", "");
						title = value[1].replaceAll("^\"|\"$", "");
						author = value[2].replaceAll("^\"|\"$", "");
						yop = value[3].replaceAll("^\"|\"$", "");
						String query = "upsert into books values ('" + isbn + "','" + title + "','" + author + "','"
								+ yop + "','','','','')";
						// System.out.println("the query is ======"+query);
						stmt.executeUpdate(query);
						con.commit();
					} else if (value.length == 3) {
						isbn = value[0].replaceAll("^\"|\"$", "");
						title = value[1].replaceAll("^\"|\"$", "");
						author = value[2].replaceAll("^\"|\"$", "");
						String query = "upsert into books values ('" + isbn + "','" + title + "','" + author
								+ "','','','','','')";
						// System.out.println("the query is ======"+query);
						stmt.executeUpdate(query);
						con.commit();
					} else if (value.length == 2) {
						isbn = value[0].replaceAll("^\"|\"$", "");
						title = value[1].replaceAll("^\"|\"$", "");
						String query = "upsert into books values ('" + isbn + "','" + title + "','','','','','','')";
						// System.out.println("the query is ======"+query);
						stmt.executeUpdate(query);
						con.commit();
					} else {
						isbn = value[0].replaceAll("^\"|\"$", "");
						String query = "upsert into books values ('" + isbn + "','','','','','','','')";
						// System.out.println("the query is ======"+query);
						stmt.executeUpdate(query);
						con.commit();
					}
					// con.commit();
				}
				// System.out.println("===========get Info========");
				// PreparedStatement statement = con.prepareStatement("select *
				// from user");
				// rset = statement.executeQuery();
				// while (rset.next()) {
				// System.out.println(rset.getString("location"));
				// }
				// statement.close();
				con.close();
				System.out.println("===All records inserted to books table!===");

			} catch (Exception e) {
				e.printStackTrace();
			}
			/*
			 * PreparedStatement statement = con.prepareStatement(
			 * "select * from test"); rset = statement.executeQuery(); while
			 * (rset.next()) { System.out.println(rset.getString("mycolumn")); }
			 * statement.close(); con.close();
			 */
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Book-Rating
		try {

			java.sql.Statement stmt = null;
			java.sql.ResultSet rset = null;
			// create table"
			stmt = con.createStatement();
			stmt.executeUpdate("drop table IF EXISTS bookRating");
			stmt.executeUpdate("create table bookRating (userIdISBN varchar not null primary key, rating varchar)");
			con.commit();
			System.out.println("bookRating table created!");
			// Insert record
			String ratingFile = "doc/BX-Book-Ratings.csv";
			File filem = new File(new File(ratingFile).getAbsolutePath());
			try {
				BufferedReader br = new BufferedReader(new FileReader(filem));
				br.readLine(); // skip first line
				String line = null;

				while ((line = br.readLine()) != null) {
					String data = line;
					data = data.replaceAll("'", "''");
					String[] value = data.split(";");
					String userId = "";
					String isbn = "";
					String rating = "";
					if (value.length == 3) {
						// movieId = Integer.parseInt(value[0]);
						userId = value[0].replaceAll("^\"|\"$", "");
						isbn = value[1].replaceAll("^\"|\"$", "");
						rating = value[2].replaceAll("^\"|\"$", "");
						stmt.executeUpdate("upsert into bookRating values ('" + userId + isbn + "','" + rating + "')");
						con.commit();
					} else {
						userId = value[0].replaceAll("^\"|\"$", "");
						isbn = value[1].replaceAll("^\"|\"$", "");
						stmt.executeUpdate("upsert into bookRating values ('" + userId + isbn + "','')");
						con.commit();
					}
					// con.commit();
				}
				// System.out.println("===========get Info========");
				// PreparedStatement statement = con.prepareStatement("select *
				// from user");
				// rset = statement.executeQuery();
				// while (rset.next()) {
				// System.out.println(rset.getString("location"));
				// }
				// statement.close();
				con.close();
				System.out.println("===All records inserted to bookRating table!===");

			} catch (Exception e) {
				e.printStackTrace();
			}
			/*
			 * PreparedStatement statement = con.prepareStatement(
			 * "select * from test"); rset = statement.executeQuery(); while
			 * (rset.next()) { System.out.println(rset.getString("mycolumn")); }
			 * statement.close(); con.close();
			 */
		} catch (Exception e) {
			e.printStackTrace();
		}

		/* Ask to perform operation */
		try {
			java.sql.Statement stmt = null;
			java.sql.ResultSet rset = null;
			Scanner sc = new Scanner(System.in);
			System.out.println("Which information you need?\n1.Users\n2.Books\n3.Book-Rating");
			String tableName = null;
			int tableChoice = sc.nextInt();
			if (tableChoice == 1) {
				tableName = "user";
			} else if (tableChoice == 2) {
				tableName = "book";
			} else if (tableChoice == 3) {
				tableName = "bookRating";
			} else {
				System.out.println("Please Input appropriate number");
				tableChoice = sc.nextInt();
			}
			PreparedStatement statement = con.prepareStatement("select * from " + tableName + "");

			System.out.println("What do you want to do?(Enter Number Please.)\n1.Get All Information.\n2.Search");
			int choice = sc.nextInt();
			if (choice == 1) {
				try {
					switch (tableChoice) {
					case 1:
						rset = statement.executeQuery();
						while (rset.next()) {
							System.out.println("userId:" + rset.getString(0) + "location:" + rset.getString(1) + "age:"
									+ rset.getShort(2));
						}
						statement.close();
						break;
					case 2:
						rset = statement.executeQuery();
						while (rset.next()) {
							System.out.println("ISBN:" + rset.getString(0) + "Book-Title:" + rset.getString(1)
									+ "Book-Author:" + rset.getString(2) + "Year-Of-Publication:" + rset.getString(3)
									+ "Publisher:" + rset.getString(4) + "Image-URL-S:" + rset.getString(5)
									+ "Image-URL-M" + rset.getString(6) + "Image-URL-L:" + rset.getString(7));
						}
						statement.close();
						break;
					case 3:
						rset = statement.executeQuery();
						while (rset.next()) {
							System.out.println("id:" + rset.getString(0) + "rating:" + rset.getString(1));
						}
						statement.close();
						break;
					default:
						System.out.println("No Sunch Info");
					}

				} catch (Exception e) {
					e.printStackTrace();
				}
			} else if (choice == 2) {
				System.out.println("Please input id:");
				String rowKey = sc.next();
				PreparedStatement st;
				try {
					switch (tableChoice) {
					case 1:
						st = con.prepareStatement("select * from " + tableName + "where userId = '" + rowKey + "'");
						rset = statement.executeQuery();
						while (rset.next()) {
							System.out.println("userId:" + rset.getString(0) + "location:" + rset.getString(1) + "age:"
									+ rset.getShort(2));
						}
						statement.close();
						break;
					case 2:
						st = con.prepareStatement("select * from " + tableName + "where ISBN = '" + rowKey + "'");
						rset = statement.executeQuery();
						while (rset.next()) {
							System.out.println("ISBN:" + rset.getString(0) + "Book-Title:" + rset.getString(1)
									+ "Book-Author:" + rset.getString(2) + "Year-Of-Publication:" + rset.getString(3)
									+ "Publisher:" + rset.getString(4) + "Image-URL-S:" + rset.getString(5)
									+ "Image-URL-M" + rset.getString(6) + "Image-URL-L:" + rset.getString(7));
						}
						statement.close();
						break;
					case 3:
						st = con.prepareStatement("select * from " + tableName + "where userIdISBN = '" + rowKey + "'");
						rset = statement.executeQuery();
						while (rset.next()) {
							System.out.println("id:" + rset.getString(0) + "rating:" + rset.getString(1));
						}
						statement.close();
						break;
					default:
						System.out.println("No Such Info");
					}

				} catch (Exception e) {
					System.out.println("No Such Information");
				}

			} else {
				System.out.println("No Such Option");
			}

			sc.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}

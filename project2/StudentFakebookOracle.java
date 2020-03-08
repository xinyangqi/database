package project2;



import java.sql.*;

/*
    The StudentFakebookOracle class is derived from the FakebookOracle class and implements
    the abstract query functions that investigate the database provided via the <connection>
    parameter of the constructor to discover specific information.
*/
public final class StudentFakebookOracle extends FakebookOracle {
    // [Constructor]
    // REQUIRES: <connection> is a valid JDBC connection
    public StudentFakebookOracle(Connection connection) {
        oracle = connection;
    }
    
    @Override
    // Query 0
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the total number of users for which a birth month is listed
    //        (B) Find the birth month in which the most users were born
    //        (C) Find the birth month in which the fewest users (at least one) were born
    //        (D) Find the IDs, first names, and last names of users born in the month
    //            identified in (B)
    //        (E) Find the IDs, first names, and last name of users born in the month
    //            identified in (C)
    //
    // This query is provided to you completed for reference. Below you will find the appropriate
    // mechanisms for opening up a statement, executing a query, walking through results, extracting
    // data, and more things that you will need to do for the remaining nine queries
    public BirthMonthInfo findMonthOfBirthInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            // Step 1
            // ------------
            // * Find the total number of users with birth month info
            // * Find the month in which the most users were born
            // * Find the month in which the fewest (but at least 1) users were born
            ResultSet rst = stmt.executeQuery(
                "SELECT COUNT(*) AS Birthed, Month_of_Birth " +         // select birth months and number of uses with that birth month
                "FROM " + UsersTable + " " +                            // from all users
                "WHERE Month_of_Birth IS NOT NULL " +                   // for which a birth month is available
                "GROUP BY Month_of_Birth " +                            // group into buckets by birth month
                "ORDER BY Birthed DESC, Month_of_Birth ASC");           // sort by users born in that month, descending; break ties by birth month
            
            int mostMonth = 0;
            int leastMonth = 0;
            int total = 0;
            while (rst.next()) {                       // step through result rows/records one by one
                if (rst.isFirst()) {                   // if first record
                    mostMonth = rst.getInt(2);         //   it is the month with the most
                }
                if (rst.isLast()) {                    // if last record
                    leastMonth = rst.getInt(2);        //   it is the month with the least
                }
                total += rst.getInt(1);                // get the first field's value as an integer
            }
            BirthMonthInfo info = new BirthMonthInfo(total, mostMonth, leastMonth);
            
            // Step 2
            // ------------
            // * Get the names of users born in the most popular birth month
            rst = stmt.executeQuery(
                "SELECT User_ID, First_Name, Last_Name " +                // select ID, first name, and last name
                "FROM " + UsersTable + " " +                              // from all users
                "WHERE Month_of_Birth = " + mostMonth + " " +             // born in the most popular birth month
                "ORDER BY User_ID");                                      // sort smaller IDs first
                
            while (rst.next()) {
                info.addMostPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 3
            // ------------
            // * Get the names of users born in the least popular birth month
            rst = stmt.executeQuery(
                "SELECT User_ID, First_Name, Last_Name " +                // select ID, first name, and last name
                "FROM " + UsersTable + " " +                              // from all users
                "WHERE Month_of_Birth = " + leastMonth + " " +            // born in the least popular birth month
                "ORDER BY User_ID");                                      // sort smaller IDs first
                
            while (rst.next()) {
                info.addLeastPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 4
            // ------------
            // * Close resources being used
            rst.close();
            stmt.close();                            // if you close the statement first, the result set gets closed automatically

            return info;

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new BirthMonthInfo(-1, -1, -1);
        }
    }
    
    @Override
    // Query 1
    // -----------------------------------------------------------------------------------
    // GOALS: (A) The first name(s) with the most letters
    //        (B) The first name(s) with the fewest letters
    //        (C) The first name held by the most users
    //        (D) The number of users whose first name is that identified in (C)
    public FirstNameInfo findNameInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*LONGEST FIRST NAME QUERY*/

            ResultSet rst=stmt.executeQuery(
                     "SELECT DISTINCT FIRST_NAME "+
                     "FROM " + UsersTable + " "+
                     " WHERE LENGTH(FIRST_NAME)= (SELECT MAX(LENGTH(FIRST_NAME)) FROM " + UsersTable + " ) "+
                     " ORDER BY FIRST_NAME");

            FirstNameInfo info = new FirstNameInfo();
            while (rst.next()){
                info.addLongName(rst.getString(1));
            }


            rst=stmt.executeQuery(
                    "SELECT DISTINCT FIRST_NAME "+
                      "FROM " + UsersTable + " "+
                      " WHERE LENGTH(FIRST_NAME)= (SELECT MIN(LENGTH(FIRST_NAME)) FROM "+ UsersTable +" ) "+
                      " ORDER BY FIRST_NAME");
            while (rst.next()){
                info.addShortName(rst.getString(1));
            }


            rst=stmt.executeQuery(
                    "SELECT FIRST_NAME, COUNT(*) "+
                        "FROM " + UsersTable + " "+
                        "GROUP BY FIRST_NAME "+
                        "HAVING COUNT(*) = (SELECT MAX(COUNT(*)) FROM " + UsersTable + "  GROUP BY FIRST_NAME) "+
                         "ORDER BY FIRST_NAME");
            while (rst.next()) {
                info.addCommonName(rst.getString(1));
                info.setCommonNameCount(rst.getInt(2));
            }

            rst.close();
            stmt.close();
            return info;
            /*

                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                FirstNameInfo info = new FirstNameInfo();
                info.addLongName("Aristophanes");
                info.addLongName("Michelangelo");
                info.addLongName("Peisistratos");
                info.addShortName("Bob");
                info.addShortName("Sue");
                info.addCommonName("Harold");
                info.addCommonName("Jessica");
                info.setCommonNameCount(42);
                return info;
            */

                         // placeholder for compilation
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new FirstNameInfo();
        }
    }
    
    @Override
    // Query 2
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users without any friends
    //
    // Be careful! Remember that if two users are friends, the Friends table only contains
    // the one entry (U1, U2) where U1 < U2.
    public FakebookArrayList<UserInfo> lonelyUsers() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
          /*  ResultSet rst=stmt.executeQuery(
                    " (SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME "+
                        "FROM " + UsersTable + " U) "+
                         "MINUS "+
                        " ( SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME"+
                        " FROM " + UsersTable + " U, " + FriendsTable + " F"+
                        " WHERE U.USER_ID=F.USER1_ID OR U.USER_ID=F.USER2_ID)"+
                        "ORDER BY 1 ASC"
            );*/
            ResultSet rst=stmt.executeQuery(
                    " SELECT USER_ID, FIRST_NAME, LAST_NAME "+
                            "FROM " + UsersTable + ""+
                            " WHERE (USER_ID NOT IN (SELECT DISTINCT USER1_ID FROM " + FriendsTable + ")) AND "+
                            " (USER_ID NOT IN (SELECT DISTINCT USER2_ID FROM " + FriendsTable + " ))"+
                            " ORDER BY USER_ID ASC");

            while (rst.next()){
                UserInfo u=new UserInfo(rst.getLong(1),rst.getString(2), rst.getString(3));
                results.add(u);
            }
            rst.close();
            stmt.close();
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(15, "Abraham", "Lincoln");
                UserInfo u2 = new UserInfo(39, "Margaret", "Thatcher");
                results.add(u1);
                results.add(u2);
            */
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 3
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users who no longer live
    //            in their hometown (i.e. their current city and their hometown are different)
    public FakebookArrayList<UserInfo> liveAwayFromHome() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            ResultSet rst = stmt.executeQuery(
                    "SELECT DISTINCT U.USER_ID, U.FIRST_NAME, U.LAST_NAME " +
                            "FROM " +  UsersTable + " U, " + CurrentCitiesTable + " C, " + HometownCitiesTable + " H " +
                            " WHERE U.USER_ID = C.USER_ID AND C.USER_ID = H.USER_ID " +
                            " AND C.CURRENT_CITY_ID <> H.HOMETOWN_CITY_ID " +
                            " ORDER BY U.USER_ID");

            long id = 0;
            String first = "";
            String last = "";
            UserInfo user = new UserInfo(id,first,last);
            while (rst.next()) {
                id = rst.getLong(1);
                first = rst.getString(2);
                last = rst.getString(3);
                user = new UserInfo(id,first,last);
                results.add(user);
            }

            rst.close();
            stmt.close();

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    
    @Override
    // Query 4
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, links, and IDs and names of the containing album of the top
    //            <num> photos with the most tagged users
    //        (B) For each photo identified in (A), find the IDs, first names, and last names
    //            of the users therein tagged
    public FakebookArrayList<TaggedPhotoInfo> findPhotosWithMostTags(int num) throws SQLException {
        FakebookArrayList<TaggedPhotoInfo> results = new FakebookArrayList<TaggedPhotoInfo>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            ResultSet rst = stmt.executeQuery(

                    " SELECT * " +
                            " FROM" +
                            " (SELECT P.PHOTO_ID, A.ALBUM_ID, A.ALBUM_LINK, A.ALBUM_NAME" +
                            " FROM " + PhotosTable + " P, " + AlbumsTable + " A," +
                            " ( SELECT T.TAG_PHOTO_ID, COUNT(*) AS COUNTING " +
                            " FROM " + TagsTable + " T GROUP BY T.TAG_PHOTO_ID) C" +
                            " WHERE P.PHOTO_ID=C.TAG_PHOTO_ID AND P.ALBUM_ID=A.ALBUM_ID" +
                            " ORDER BY C.COUNTING DESC, P.PHOTO_ID ASC)" +
                            " WHERE ROWNUM<=" + num + "");

            /*ODER BY????*/
            /* String q = "SELECT name FROM Students WHERE age = ?";
            if (!ps) ps = conn.prepareStatement(q);
            ps.setDouble(1, age);
            ResultSet rs = ps.executeQuery();*/

            /* stmt.executeQuery("drop view tagged");*/

            while (rst.next()) {
                PhotoInfo p = new PhotoInfo(rst.getLong(1), rst.getInt(2), rst.getString(3), rst.getString(4));
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
                long id = rst.getLong(1);
                Statement stmt2 = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly);
                ResultSet rst2 = stmt2.executeQuery("(SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME " +
                        "FROM " + TagsTable + " T, " + UsersTable + " U" +
                        " WHERE T.TAG_PHOTO_ID= " + id + " AND T.TAG_SUBJECT_ID=U.USER_ID)" +
                        " ORDER BY U.USER_ID ASC");
                while (rst2.next()) {
                    UserInfo u = new UserInfo(rst2.getLong(1), rst2.getString(2), rst2.getString(3));
                    tp.addTaggedUser(u);
                }
                rst2.close();
                stmt2.close();
                results.add(tp);
            }
            rst.close();
            stmt.close();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return results;
    }

    


    // Query 5
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, last names, and birth years of each of the two
    //            users in the top <num> pairs of users that meet each of the following
    //            criteria:
    //              (i) same gender
    //              (ii) tagged in at least one common photo
    //              (iii) difference in birth years is no more than <yearDiff>
    //              (iv) not friends
    //        (B) For each pair identified in (A), find the IDs, links, and IDs and names of
    //            the containing album of each photo in which they are tagged together
    @Override
    // Query 5
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, last names, and birth years of each of the two
    //            users in the top <num> pairs of users that meet each of the following
    //            criteria:
    //              (i) same gender
    //              (ii) tagged in at least one common photo
    //              (iii) difference in birth years is no more than <yearDiff>
    //              (iv) not friends
    //        (B) For each pair identified in (A), find the IDs, links, and IDs and names of
    //            the containing album of each photo in which they are tagged together
    public FakebookArrayList<MatchPair> matchMaker(int num, int yearDiff) throws SQLException {
        FakebookArrayList<MatchPair> results = new FakebookArrayList<MatchPair>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
          /*  stmt.executeQuery(
                    " CREATE VIEW NOT_FRIENDS AS " +
                            " SELECT U1.USER_ID AS USER1_ID, U2.USER_ID AS USER2_ID, U1.GENDER AS USER1_GENDER, U2.GENDER AS USER2_GENDER, " +
                            " U1.YEAR_OF_BIRTH AS USER1_YEAR, U2.YEAR_OF_BIRTH AS USER2_YEAR, U1.FIRST_NAME AS USER1_FIRST, " +
                            " U2.FIRST_NAME AS USER2_FIRST, U1.LAST_NAME AS USER1_LAST, U2.LAST_NAME AS USER2_LAST " +
                            " FROM " + UsersTable + " U1, " + UsersTable + " U2 " +
                            " WHERE U1.USER_ID < U2.USER_ID AND " +
                            " (U1.USER_ID, U2.USER_ID) NOT IN (SELECT DISTINCT F.USER1_ID, F.USER2_ID " +
                            " FROM " + FriendsTable + " F ) ");*/

            ResultSet rst = stmt.executeQuery(
                    " SELECT * FROM (SELECT DISTINCT U1.USER_ID AS USER1_ID, U2.USER_ID AS USER2_ID, " +
                            " U1.YEAR_OF_BIRTH AS USER1_YEAR, U2.YEAR_OF_BIRTH AS USER2_YEAR, U1.FIRST_NAME AS USER1_FIRST, " +
                            " U2.FIRST_NAME AS USER2_FIRST, U1.LAST_NAME AS USER1_LAST, U2.LAST_NAME AS USER2_LAST " +
                            " FROM " + UsersTable + " U1 JOIN " + TagsTable + " T1 " + " ON U1.USER_ID = T1.TAG_SUBJECT_ID " +
                            " JOIN " + TagsTable + " T2 " + " ON T2.TAG_PHOTO_ID = T1.TAG_PHOTO_ID " +
                            " JOIN " + UsersTable + " U2 ON U2.USER_ID = T2.TAG_SUBJECT_ID " +
                            " WHERE U1.GENDER = U2.GENDER " + " AND " + " U1.USER_ID < U2.USER_ID " +
                            " AND (U1.YEAR_OF_BIRTH - U2.YEAR_OF_BIRTH) <= " + yearDiff +
                            " AND (U1.YEAR_OF_BIRTH - U2.YEAR_OF_BIRTH) >= " + (-1)*yearDiff +
                            " AND NOT EXISTS ( SELECT F.USER1_ID, F.USER2_ID FROM " + FriendsTable + " F WHERE F.USER1_ID = U1.USER_ID AND F.USER2_ID = U2.USER_ID " + ") " +
                            " GROUP BY ( U1.USER_ID , U2.USER_ID , U1.GENDER , U2.GENDER , " +
                            "  U1.YEAR_OF_BIRTH , U2.YEAR_OF_BIRTH, U1.FIRST_NAME, " +
                            "  U2.FIRST_NAME , U1.LAST_NAME , U2.LAST_NAME ) " +
                            " ORDER BY COUNT(*), U1.USER_ID, U2.USER_ID ) " +
                            " WHERE ROWNUM <= " + num);


            UserInfo u1; long u1_id = 0; String u1_first = ""; String u1_last = ""; int u1_year = 0;
            UserInfo u2; long u2_id = 0; String u2_first = ""; String u2_last = ""; int u2_year = 0;
            long p_id = 0; long a_id = 0; String a_name = ""; String link = "";
            MatchPair mp;
            PhotoInfo p;

            while (rst.next()) {
                u1_id = rst.getLong(1);
                u2_id = rst.getLong(2);
                u1_first = rst.getString(5);
                u2_first = rst.getString(6);
                u1_last = rst.getString(7);
                u2_last = rst.getString(8);
                u1_year = rst.getInt(3);
                u2_year = rst.getInt(4);

                u1 = new UserInfo(u1_id, u1_first, u1_last);
                u2 = new UserInfo(u2_id, u2_first, u2_last);
                mp = new MatchPair(u1, u1_year, u2, u2_year);

                Statement stmt3 = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly);
                ResultSet rst1;
                rst1 = stmt3.executeQuery(
                        " SELECT DISTINCT P.PHOTO_ID, P.PHOTO_LINK, P.ALBUM_ID, A.ALBUM_NAME " +
                                " FROM " + PhotosTable + " P JOIN " + AlbumsTable + " A " + " ON P.ALBUM_ID = A.ALBUM_ID" +
                                " JOIN " + TagsTable + " T1 ON T1.TAG_PHOTO_ID = P.PHOTO_ID " +
                                " JOIN " + TagsTable + " T2 ON T2.TAG_PHOTO_ID = P.PHOTO_ID " +
                                " WHERE T1.TAG_SUBJECT_ID = " + u1_id +
                                " AND T2.TAG_SUBJECT_ID = " + u2_id +
                                " ORDER BY P.PHOTO_ID ");

                while (rst1.next()) {
                    p_id = rst1.getLong(1);
                    link = rst1.getString(2);
                    a_id = rst1.getLong(3);
                    a_name = rst1.getString(4);
                    p = new PhotoInfo(p_id, a_id, link, a_name);
                    mp.addSharedPhoto(p);
                }
                rst1.close();
                stmt3.close();
                results.add(mp);
            }
            rst.close();
            stmt.close();
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }
    @Override
    // Query 6
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of each of the two users in
    //            the top <num> pairs of users who are not friends but have a lot of
    //            common friends
    //        (B) For each pair identified in (A), find the IDs, first names, and last names
    //            of all the two users' common friends
    public FakebookArrayList<UsersPair> suggestFriends(int num) throws SQLException {
        FakebookArrayList<UsersPair> results = new FakebookArrayList<UsersPair>("\n");
        
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            Statement stmt2=oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly);
            Statement stmt4=oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly);


          stmt.executeQuery(
                    "CREATE VIEW PAIRS AS "+
                            " (SELECT USER1_ID AS USER1, USER2_ID AS USER2 FROM " + FriendsTable + ")"+
                            " UNION "+
                            " (SELECT USER2_ID AS USER1, USER1_ID AS USER2 FROM " + FriendsTable + ")"

          );


           ResultSet rst1=stmt2.executeQuery("SELECT MUTUAL.COUNT, U1.USER_ID AS USER1, U1.FIRST_NAME AS FIRST1, U1.LAST_NAME AS LAST1, U2.USER_ID AS USER2, " +
                           " U2.FIRST_NAME AS FIRST2, U2.FIRST_NAME AS LAST2, U3.USER_ID AS FID, U3.FIRST_NAME AS FFIRST, U3.LAST_NAME AS FLAST  " +
                           " FROM "+
                   " (SELECT * FROM "+
                   "(SELECT P1.USER1, P2.USER2, COUNT(*) AS COUNT "+
                       " FROM PAIRS P1 JOIN PAIRS P2 ON P1.USER2=P2.USER1 " +
                      "WHERE P1.USER1<P2.USER2 AND " +
                      " NOT EXISTS (SELECT * FROM " + FriendsTable + " F1 WHERE F1.USER1_ID=P1.USER1 AND F1.USER2_ID=P2.USER2 )"+
                      " GROUP BY P1.USER1, P2.USER2 "+
                       " ORDER BY COUNT(*) DESC, P1.USER1 , P2.USER2) "+
                       " WHERE ROWNUM<= " + num + ") MUTUAL "+
                   " JOIN " + UsersTable + " U1 ON U1.USER_ID=MUTUAL.USER1 "+
                    " JOIN " + UsersTable + " U2 ON U2.USER_ID=MUTUAL.USER2 "+
                    " JOIN PAIRS P ON P.USER1=MUTUAL.USER1"+
                    " JOIN PAIRS P2 ON P2.USER1=MUTUAL.USER2"+
                    " JOIN " + UsersTable + " U3 ON U3.USER_ID=P.USER2 "+
                    " WHERE P.USER2=P2.USER2"+
                    " ORDER BY MUTUAL.COUNT DESC, MUTUAL.USER1, MUTUAL.USER2, P.USER2"
           );


             while (rst1.next() ){
             UserInfo u1=new UserInfo(rst1.getLong(2),rst1.getString(3),rst1.getString(4));
             UserInfo u2=new UserInfo(rst1.getLong(5),rst1.getString(6),rst1.getString(7));
             int count=rst1.getInt(1);
             UsersPair up=new UsersPair(u1,u2);

             UserInfo u=new UserInfo(rst1.getInt(8), rst1.getString(9),rst1.getString(10));
             up.addSharedFriend(u);
             count--;
             while (count>=1){
                    rst1.next();
                    u=new UserInfo(rst1.getInt(8), rst1.getString(9),rst1.getString(10));
                    up.addSharedFriend(u);
                 count--;
                }
                 results.add(up);

            }
             stmt4.executeQuery("DROP VIEW PAIRS");

             rst1.close();
             stmt.close();

             stmt2.close();
             stmt4.close();
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(16, "The", "Hacker");
                UserInfo u2 = new UserInfo(80, "Dr.", "Marbles");
                UserInfo u3 = new UserInfo(192, "Digit", "Le Boid");
                UsersPair up = new UsersPair(u1, u2);
                up.addSharedFriend(u3);
                results.add(up);
            */
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        
        return results;
    }
    
    @Override
    // Query 7
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the name of the state or states in which the most events are held
    //        (B) Find the number of events held in the states identified in (A)
    public EventStateInfo findEventStates() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                EventStateInfo info = new EventStateInfo(50);
                info.addState("Kentucky");
                info.addState("Hawaii");
                info.addState("New Hampshire");
                return info;
            */
            ResultSet rst = stmt.executeQuery(
                    "SELECT C.STATE_NAME AS STATE, COUNT(*) " +
                            " FROM " +  EventsTable + " E, " + CitiesTable + " C " +
                            " WHERE E.EVENT_CITY_ID = C.CITY_ID" +
                            " GROUP BY C.STATE_NAME" +
                            " HAVING COUNT(*) = (SELECT MAX(COUNT(*))" +
                            " FROM " +  EventsTable + " E2, " + CitiesTable + " C2 " +
                            "                   WHERE E2.EVENT_CITY_ID = C2.CITY_ID" +
                            "                   GROUP BY C2.STATE_NAME)" +
                            " ORDER BY C.STATE_NAME");

            EventStateInfo info = new EventStateInfo(0);
            while (rst.next()){
                if (rst.isFirst()) {
                    long count = rst.getLong(2);
                    info = new EventStateInfo(count);
                }
                info.addState(rst.getString(1));
            }

            rst.close();
            stmt.close();

            return info;

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new EventStateInfo(-1);
        }
    }


    @Override
    // Query 8
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the ID, first name, and last name of the oldest friend of the user
    //            with User ID <userID>
    //        (B) Find the ID, first name, and last name of the youngest friend of the user
    //            with User ID <userID>
    public AgeInfo findAgeInfo(long userID) throws SQLException {
       try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {
            ResultSet rst=stmt.executeQuery(

                     "SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME FROM " + FriendsTable+ " F, "+ UsersTable + " U "+
                     "WHERE (F.USER1_ID=" + userID + " AND F.USER2_ID=U.USER_ID)" +
                     " OR "+
                      " (F.USER2_ID=" + userID + " AND F.USER1_ID=U.USER_ID) "+
                     " ORDER BY U.YEAR_OF_BIRTH DESC, U.MONTH_OF_BIRTH DESC, U.DAY_OF_BIRTH DESC, U.USER_ID DESC"
            );
         /*  ResultSet rst=stmt.executeQuery(

                    "SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME FROM  "+ UsersTable + " U "+
                            "WHERE (U.USER_ID IN (SELECT USER1_ID FROM " +  FriendsTable + " WHERE USER2_ID=" + userID + "))" +
                            " OR "+
                            " (U.USER_ID IN (SELECT USER2_ID FROM " + FriendsTable + " WHERE USER1_ID=" + userID + "))  "+
                            " ORDER BY U.YEAR_OF_BIRTH DESC, U.MONTH_OF_BIRTH DESC, U.DAY_OF_BIRTH DESC, U.USER_ID DESC"
            );*/
            long id1=0;
            long id2=0;
            String first1=null;
            String first2=null;
            String last1=null;
            String last2=null;
            while (rst.next()){
                if (rst.isFirst()) {id1=rst.getLong(1);first1=rst.getString(2);last1=rst.getString(3);}
                if (rst.isLast()) {id2=rst.getLong(1);first2=rst.getString(2);last2=rst.getString(3);}
            }
            rst.close();
            stmt.close();
            UserInfo young = new UserInfo(id1,first1,last1);
            UserInfo old = new UserInfo(id2, first2, last2);
            return new AgeInfo(old, young);

            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo old = new UserInfo(12000000, "Galileo", "Galilei");
                UserInfo young = new UserInfo(80000000, "Neil", "deGrasse Tyson");
                return new AgeInfo(old, young);
            */
                           // placeholder for compilation
        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
            return new AgeInfo(new UserInfo(-1, "ERROR", "ERROR"), new UserInfo(-1, "ERROR", "ERROR"));
        }
    }
    
    @Override
    // Query 9
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find all pairs of users that meet each of the following criteria
    //              (i) same last name
    //              (ii) same hometown
    //              (iii) are friends
    //              (iv) less than 10 birth years apart
    public FakebookArrayList<SiblingInfo> findPotentialSiblings() throws SQLException {
        FakebookArrayList<SiblingInfo> results = new FakebookArrayList<SiblingInfo>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly)) {


            ResultSet rst = stmt.executeQuery(
                    "SELECT DISTINCT F.USER1_ID, U1.FIRST_NAME, U1.LAST_NAME, F.USER2_ID, U2.FIRST_NAME, U2.LAST_NAME" +
                            " FROM " + FriendsTable + " F " +
                            " JOIN " + UsersTable + " U1 ON F.USER1_ID=U1.USER_ID "+
                            " JOIN " + UsersTable + " U2 ON F.USER2_ID = U2.USER_ID"+
                            " JOIN " + HometownCitiesTable + " H1 ON H1.USER_ID = U1.USER_ID "+
                            " JOIN " + HometownCitiesTable + " H2 ON H2.USER_ID = U2.USER_ID "+
                            " WHERE " +
                            " H1.HOMETOWN_CITY_ID = H2.HOMETOWN_CITY_ID AND U1.LAST_NAME = U2.LAST_NAME AND" +
                            " (U1.YEAR_OF_BIRTH - U2.YEAR_OF_BIRTH) < 10 " +
                            " AND (U1.YEAR_OF_BIRTH - U2.YEAR_OF_BIRTH) > -10 "+
                            " ORDER BY F.USER1_ID, F.USER2_ID ");

            long u1_id = 0;
            long u2_id = 0;
            String u1_first = " ";
            String u1_last = " ";
            String u2_first = " ";
            String u2_last = " ";
            UserInfo u1 = new UserInfo(u1_id, u1_first, u1_last);
            UserInfo u2 = new UserInfo(u2_id, u2_first, u2_last);
            SiblingInfo s1 = new SiblingInfo(u1, u2);

            while (rst.next()){
                u1_id = rst.getLong(1);
                u2_id = rst.getLong(4);
                u1_first = rst.getString(2);
                u2_first = rst.getString(5);
                u1_last = rst.getString(3);
                u2_last = rst.getString(6);
                u1 = new UserInfo(u1_id, u1_first, u1_last);
                u2 = new UserInfo(u2_id, u2_first, u2_last);
                s1 = new SiblingInfo(u1, u2);
                results.add(s1);
            }

            rst.close();
            stmt.close();

        }
        catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }



    // Member Variables
    private Connection oracle;
    private final String UsersTable = FakebookOracleConstants.UsersTable;
    private final String CitiesTable = FakebookOracleConstants.CitiesTable;
    private final String FriendsTable = FakebookOracleConstants.FriendsTable;
    private final String CurrentCitiesTable = FakebookOracleConstants.CurrentCitiesTable;
    private final String HometownCitiesTable = FakebookOracleConstants.HometownCitiesTable;
    private final String ProgramsTable = FakebookOracleConstants.ProgramsTable;
    private final String EducationTable = FakebookOracleConstants.EducationTable;
    private final String EventsTable = FakebookOracleConstants.EventsTable;
    private final String AlbumsTable = FakebookOracleConstants.AlbumsTable;
    private final String PhotosTable = FakebookOracleConstants.PhotosTable;
    private final String TagsTable = FakebookOracleConstants.TagsTable;
}

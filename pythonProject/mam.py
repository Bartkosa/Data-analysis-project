### Data Processing in R and Python 2022Z
### Homework Assignment no. 2
###
### IMPORTANT
### This file should contain only solutions to tasks in the form of a functions
### definitions and comments to the code.
import numpy as np
import pandas as pd
import os.path
import sqlite3
import tempfile
from Kosieradzki_Bartosz_assignment_2 import *

Posts = pd.read_csv(r"Posts.csv")
Badges = pd.read_csv(r"Badges.csv")
Comments = pd.read_csv(r"Comments.csv")
Users = pd.read_csv(r"Users.csv")
Votes = pd.read_csv(r"Votes.csv")

baza = os.path.join(tempfile.mkdtemp(), 'example.db')
if os.path.isfile(baza):
    os.remove(baza)

conn = sqlite3.connect(baza)

Badges.to_sql("Badges", conn)
Comments.to_sql("Comments", conn)
Posts.to_sql("Posts", conn)
Users.to_sql("Users", conn)
Votes.to_sql("Votes", conn)

res1 = pd.read_sql_query("""
                        SELECT STRFTIME('%Y', CreationDate) AS Year, COUNT(*) AS TotalNumber
                           FROM Posts
                           GROUP BY Year
                        """, conn)

res2 = pd.read_sql_query("""
                                    SELECT Id, DisplayName, SUM(ViewCount) AS TotalViews
                                    FROM Users
                                    JOIN (
                                    SELECT OwnerUserId, ViewCount FROM Posts WHERE PostTypeId = 1
                                    ) AS Questions
                                    ON Users.Id = Questions.OwnerUserId
                                    GROUP BY Id
                                    ORDER BY TotalViews DESC
                                    LIMIT 10
                                    """, conn)
res3_sql = pd.read_sql_query("""
                                    SELECT Year, Name, MAX((Count * 1.0) / CountTotal) AS MaxPercentage
                                    FROM (
                                    SELECT BadgesNames.Year, BadgesNames.Name, BadgesNames.Count, BadgesYearly.CountTotal
                                    FROM (
                                    SELECT Name, COUNT(*) AS Count, STRFTIME('%Y', Badges.Date) AS Year
                                    FROM Badges
                                    GROUP BY Name, Year
                                    ) AS BadgesNames
                                    JOIN (
                                    SELECT COUNT(*) AS CountTotal, STRFTIME('%Y', Badges.Date) AS Year
                                    FROM Badges
                                    GROUP BY YEAR
                                    ) AS BadgesYearly
                                    ON BadgesNames.Year = BadgesYearly.Year
                                    )
                                    GROUP BY Year
                                    """, conn)
res4_sql = pd.read_sql_query("""
                                    SELECT Title, CommentCount, ViewCount, CommentsTotalScore, DisplayName, Reputation, Location
                                    FROM (
                                    SELECT Posts.OwnerUserId, Posts.Title, Posts.CommentCount, Posts.ViewCount,
                                    CmtTotScr.CommentsTotalScore
                                    FROM (
                                    SELECT PostId, SUM(Score) AS CommentsTotalScore
                                    FROM Comments
                                    GROUP BY PostId
                                    ) AS CmtTotScr
                                    JOIN Posts ON Posts.Id = CmtTotScr.PostId
                                    WHERE Posts.PostTypeId=1
                                    ) AS PostsBestComments
                                    JOIN Users ON PostsBestComments.OwnerUserId = Users.Id
                                    ORDER BY CommentsTotalScore DESC LIMIT 10
                                    """, conn)
res5_sql = pd.read_sql_query("""
                                        SELECT Posts.Title, STRFTIME('%Y-%m-%d', Posts.CreationDate) AS Date, VotesByAge.*
                                        FROM Posts
                                        JOIN (
                                        SELECT PostId,
                                        MAX(CASE WHEN VoteDate = 'before' THEN Total ELSE 0 END) BeforeCOVIDVotes,
                                        MAX(CASE WHEN VoteDate = 'during' THEN Total ELSE 0 END) DuringCOVIDVotes,
                                        MAX(CASE WHEN VoteDate = 'after' THEN Total ELSE 0 END) AfterCOVIDVotes,
                                        SUM(Total) AS Votes
                                        FROM (
                                        SELECT PostId,
                                        CASE STRFTIME('%Y', CreationDate)
                                        WHEN '2022' THEN 'after'
                                        WHEN '2021' THEN 'during'
                                        WHEN '2020' THEN 'during'
                                        WHEN '2019' THEN 'during'
                                        ELSE 'before'
                                        END VoteDate, COUNT(*) AS Total
                                        FROM Votes
                                        WHERE VoteTypeId IN (3, 4, 12)
                                        GROUP BY PostId, VoteDate
                                        ) AS VotesDates
                                        GROUP BY VotesDates.PostId
                                        ) AS VotesByAge ON Posts.Id = VotesByAge.PostId
                                        WHERE Title NOT IN ('') AND DuringCOVIDVotes > 0
                                        ORDER BY DuringCOVIDVotes DESC, Votes DESC
                                        LIMIT 20
                                        """, conn)
#
# -----------------------------------------------------------------------------#
# Task 1
# -----------------------------------------------------------------------------#


def solution_1(Posts):
    posts2 = Posts
    posts2['CreationDate'] = pd.to_datetime(posts2['CreationDate'])
    posts2['Year'] = posts2['CreationDate'].dt.year

    res = posts2.groupby('Year').size().reset_index(name='TotalNumber')
    res['Year'] = res['Year'].astype('str')

    return res

res1.equals(solution_1(Posts))


# -----------------------------------------------------------------------------#
# Task 2
# -----------------------------------------------------------------------------#


def solution_2(Users, Posts):


    users2 = Users[['Id', 'DisplayName']]
    posts2 = Posts[Posts['PostTypeId'] == 1]
    posts2 = posts2.loc[:, ['OwnerUserId', 'ViewCount']]
    posts2 = posts2.groupby("OwnerUserId").sum().reset_index()
    posts2 = posts2.rename({'ViewCount': 'TotalViews', 'OwnerUserId': 'Id'}, axis='columns')

    res = pd.merge(users2, posts2, on='Id')
    res = res.sort_values(by='TotalViews', ascending=False).reset_index(drop=True)
    return res.head(10)

res2.equals(solution_2(Users, Posts))


# -----------------------------------------------------------------------------#
# Task 3
# -----------------------------------------------------------------------------#


def solution_3(Badges):
    badgesNames = Badges
    badgesNames['Date'] = pd.to_datetime(badgesNames['Date'])
    badgesNames['Year'] = badgesNames['Date'].dt.year
    badgesNames = badgesNames.groupby(['Year', 'Name']).size().reset_index(name='Count')

    badgesYearly = Badges
    badgesYearly['Date'] = pd.to_datetime(badgesYearly['Date'])
    badgesYearly['Year'] = badgesYearly['Date'].dt.year
    badgesYearly = badgesYearly.groupby('Year').size().reset_index(name='CountTotal')

    mergedBadges = pd.merge(badgesNames, badgesYearly, on='Year')
    mergedBadges = mergedBadges.sort_values(by='Name', ascending=True).reset_index(drop=True)
    mergedBadges['x'] = mergedBadges['Count'] * 1.0 / mergedBadges['CountTotal']

    res = mergedBadges.groupby('Year')['x'].max().reset_index()
    res = pd.merge(res, mergedBadges[['x', 'Name']], on='x')
    res = res.sort_values(by='Year', ascending=True).reset_index(drop=True)
    res = res.rename({'x': 'MaxPercentage'}, axis='columns')
    res = res[['Year', 'Name', 'MaxPercentage']]
    res['Year'] = res['Year'].astype('str')
    return res

res3_sql.equals(solution_3(Badges))

# -----------------------------------------------------------------------------#
# Task 4
# -----------------------------------------------------------------------------#


def solution_4(Comments, Posts, Users):
    CmtTotScr = Comments.groupby('PostId')['Score'].sum().reset_index()
    CmtTotScr = CmtTotScr.rename({'PostId': 'Id', 'Score': 'CommentsTotalScore'}, axis='columns')

    posts2 = Posts[Posts['PostTypeId'] == 1]
    postsBestComments = pd.merge(CmtTotScr, posts2, on='Id')
    postsBestComments = postsBestComments.loc[:, ['OwnerUserId', 'Title', 'CommentCount', 'ViewCount',
                                                  'CommentsTotalScore']]

    res = pd.merge(postsBestComments, Users, left_on='OwnerUserId', right_on='Id')
    res = res.sort_values(by='CommentsTotalScore', ascending=False).reset_index(drop=True)
    res = res.loc[:, ['Title', 'CommentCount', 'ViewCount', 'CommentsTotalScore',
                      'DisplayName', 'Reputation', 'Location']]
    return res.head(10)




# -----------------------------------------------------------------------------#
# Task 5
# -----------------------------------------------------------------------------#


def solution_5(Posts, Votes):
    VoteDates = Votes
    VoteDates["year"] = pd.to_datetime(VoteDates["CreationDate"]).dt.year
    VoteDates["VoteDate"] = "before"
    VoteDates.loc[VoteDates["year"] == 2022, "VoteDate"] = "after"
    VoteDates.loc[VoteDates["year"].isin([2019, 2020, 2021]), "VoteDate"] = "during"
    VoteDates = VoteDates[VoteDates["VoteTypeId"].isin([3, 4, 12])][["PostId", "VoteDate"]]
    VoteDates = VoteDates.groupby(["PostId", "VoteDate"]).size().reset_index(name="Total")

    BeforeCovid = VoteDates[VoteDates["VoteDate"] == "before"][["PostId", "Total"]]\
        .rename(columns={"Total": "BeforeCOVIDVotes"})

    DuringCovid = VoteDates[VoteDates["VoteDate"] == "during"][["PostId", "Total"]] \
        .rename(columns={"Total": "DuringCOVIDVotes"})

    AfterCovid = VoteDates[VoteDates["VoteDate"] == "after"][["PostId", "Total"]] \
        .rename(columns={"Total": "AfterCOVIDVotes"})

    VotesByAge = pd.concat([BeforeCovid, DuringCovid, AfterCovid], ignore_index=True).fillna(0)
    VotesByAge["Votes"] = VotesByAge["BeforeCOVIDVotes"] + \
                          VotesByAge["DuringCOVIDVotes"] + VotesByAge["AfterCOVIDVotes"]

    res = Posts[["Id", "Title", "CreationDate"]].rename(columns={"Id": "PostId", "CreationDate": "Date"})
    res["Date"] = pd.to_datetime(res["Date"]).dt.strftime("%Y-%m-%d")
    res = pd.merge(res, VotesByAge, on='PostId')
    res = res[(~pd.isnull(res)) & (res["DuringCOVIDVotes"] > 0)]
    res = res.sort_values(by=["DuringCOVIDVotes", "Votes"], ascending=[False, False]).reset_index(drop=True)
    column = res.pop("PostId")
    res.insert(0, "PostId", column)
    return res.head(20)


reseses = solution_5(Posts, Votes)
solution_5(Posts,Votes).equals(res5_sql)


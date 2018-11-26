import pandas as pd

'''
The table below was created manually using the report "State of Washington Members of the Legislature
1889-2018". The report can be found here: http://leg.wa.gov/History/Legislative/Documents/MembersOfLeg2018.pdf.

The missing legislators were identified by recording which legislators were included in wa_leg_raw: vote_api, 
but were not included in wa_leg_raw: committee_member_api. This meant that party and district information were
missing for these legislators.
'''

missing_legislators = [{'name': 'Ann Anderson', 'voter_id': '7', 'district': '42', 'party': 'R'},
 {'name': 'Bob Basich', 'voter_id': '23', 'district': '19', 'party': 'D'},
 {'name': 'John Beck', 'voter_id': '32', 'district': '21', 'party': 'R'},
 {'name': 'Jennifer Belcher', 'voter_id': '34', 'district': '22', 'party': 'D'},
 {'name': 'John Betrozoff', 'voter_id': '40', 'district': '45', 'party': 'R'},
 {'name': 'Rose Bowman', 'voter_id': '45', 'district': '20', 'party': 'R'},
 {'name': 'Dennis Braddock', 'voter_id': '46',  'district': '42', 'party': 'D'},
 {'name': 'Lane Bray', 'voter_id': '47', 'district': '8', 'party': 'D'},
 {'name': 'Joanne Brekke', 'voter_id': '48', 'district': '32', 'party': 'D'},
 {'name': 'Art Broback', 'voter_id': '50', 'district': '28', 'party': 'R'},
 {'name': 'Jean-Marie Brough', 'voter_id': '54', 'district': '30', 'party': 'R'},
 {'name': 'Bill Brumsickle', 'voter_id': '63', 'district': '20', 'party': 'R'},
 {'name': 'Maria Cantwell', 'voter_id': '68', 'district': '44', 'party': 'D'},
 {'name': 'Gary Chandler', 'voter_id': '75', 'district': '13', 'party': 'R'},
 {'name': 'David Cooper', 'voter_id': '93', 'district': '18', 'party': 'D'},
 {'name': 'Bill Day', 'voter_id': '103', 'district': '3', 'party': 'D'},
 {'name': 'Dennis Dellwo', 'voter_id': '110', 'district': '3', 'party': 'D'},
 {'name': 'Randy Dorn', 'voter_id': '116', 'district': '2', 'party': 'D'},
 {'name': 'Brian Ebersole', 'voter_id': '121', 'district': '29', 'party': 'D'}, 
 {'name': 'Betty Edmondson', 'voter_id': '123', 'district': '14', 'party': 'R'},
 {'name': 'Roy Ferguson', 'voter_id': '132', 'district': '48', 'party': 'R'},
 {'name': 'Greg Fisher', 'voter_id': '135', 'district': '33', 'party': 'D'},
 {'name': 'Elmira Forner', 'voter_id': '140', 'district': '47', 'party': 'R'}, 
 {'name': 'Steve Fuhrman', 'voter_id': '146', 'district': '7', 'party': 'R'},
 {'name': 'Lorraine Hine', 'voter_id': '186', 'district': '33', 'party': 'D'},
 {'name': 'Bruce Holland', 'voter_id': '192', 'district': '47', 'party': 'R'},
 {'name': 'Jay Inslee', 'voter_id': '205', 'district': '14', 'party': 'D'},
 {'name': 'Rob Johnson', 'voter_id': '214', 'district': '40', 'party': 'D'},
 {'name': 'Evan Jones', 'voter_id': '219', 'district': '24', 'party': 'D'},
 {'name': 'Joseph King', 'voter_id': '226', 'district': '49', 'party': 'D'},
 {'name': 'Richard King', 'voter_id': '227', 'district': '38', 'party': 'D'},
 {'name': 'Pete Kremen', 'voter_id': '234', 'district': '42', 'party': 'D'},
 {'name': 'June Leonard', 'voter_id': '245', 'district': '11', 'party': 'D'},
 {'name': 'Gary Locke', 'voter_id': '251', 'district': '37', 'party': 'D'},
 {'name': 'Curtis Ludwig', 'voter_id': '254', 'district': '8', 'party': 'D'},
 {'name': 'Fred May', 'voter_id': '264', 'district': '41', 'party': 'R'},
 {'name': 'Alex McLean', 'voter_id': '276', 'district': '12', 'party': 'R'},
 {'name': 'Holly Meyers', 'voter_id': '282', 'district': '17', 'party': 'D'},
 {'name': 'Todd Mielke', 'voter_id': '284', 'district': '5', 'party': 'R'},
 {'name': 'Todd Mielke', 'voter_id': '284', 'district': '6', 'party': 'R'},
 {'name': 'Louise Miller', 'voter_id': '285', 'district': '45', 'party': 'R'},
 {'name': 'John Moyer', 'voter_id': '297', 'district': '6', 'party': 'R'},
 {'name': 'John Moyer', 'voter_id': '297', 'district': '3', 'party': 'R'},
 {'name': 'Harvey Myers', 'voter_id': '301', 'district': '15', 'party': 'R'},
 {'name': 'Darwin Nealey', 'voter_id': '304', 'district': '9', 'party': 'R'},
 {'name': "John O'Brien", 'voter_id': '311', 'district': '37', 'party': 'D'},
 {'name': 'George Orr', 'voter_id': '320', 'district': '4', 'party': 'D'},
 {'name': 'George-Marshall Paris', 'voter_id': '325', 'district': '44', 'party': 'R'},
 {'name': 'W-Kim Peery', 'voter_id': '328', 'district': '17', 'party': 'D'},
 {'name': 'Larry Phillips', 'voter_id': '332', 'district': '36', 'party': 'D'},
 {'name': 'Wes Pruitt', 'voter_id': '341', 'district': '26', 'party': 'D'},
 {'name': 'Margaret Rayburn', 'voter_id': '349', 'district': '15', 'party': 'D'},
 {'name': 'Mike Riley', 'voter_id': '359', 'district': '19', 'party': 'D'},
 {'name': 'Judith Roland', 'voter_id': '363', 'district': '31', 'party': 'D'},
 {'name': 'Nancy Rust', 'voter_id': '367', 'district': '1', 'party': 'D'},
 {'name': 'Karen Schmidt', 'voter_id': '376', 'district': '23', 'party': 'R'},
 {'name': 'B-Jean Silver', 'voter_id': '396', 'district': '5', 'party': 'R'},
 {'name': 'B-Jean Silver', 'voter_id': '396', 'district': '6', 'party': 'R'},
 {'name': 'Duane Sommers, ', 'voter_id': '406', 'district': '6', 'party': 'R'},
 {'name': 'Art Sprenkle', 'voter_id': '410', 'district': '39', 'party': 'D'},
 {'name': 'Randy Tate', 'voter_id': '429', 'district': '25', 'party': 'R'},
 {'name': 'Christopher Vance', 'voter_id': '444', 'district': '31', 'party': 'R'}, 
 {'name': 'Art Wang', 'voter_id': '449', 'district': '27', 'party': 'D'},
 {'name': 'Simeon Wilson', 'voter_id': '472', 'district': '10', 'party': 'R'},
 {'name': 'Jesse Wineberry', 'voter_id': '474', 'district': '43', 'party': 'D'},
 {'name': 'Jesse Wineberry', 'voter_id': '474', 'district': '37', 'party': 'D'},
 {'name': 'John Wynne', 'voter_id': '481', 'district': '39', 'party': 'R'},
 {'name': 'Neil Amondson', 'voter_id': '5', 'district': '20', 'party': 'R'},
 {'name': 'Cliff Bailey', 'voter_id': '15', 'district': '39', 'party': 'R'},
 {'name': 'Scott Barr', 'voter_id': '19', 'district': '7', 'party': 'R'},
 {'name': 'Albert Bauer', 'voter_id': '25', 'district': '49', 'party': 'D'},
 {'name': 'Alan Bluechel', 'voter_id': '42', 'district': '45', 'party': 'R'},
 {'name': 'Emilio Cantu', 'voter_id': '67', 'district': '41', 'party': 'R'},
 {'name': 'Paul Conner', 'voter_id': '90', 'district': '24', 'party': 'D'},
 {'name': 'Ellen Craswell', 'voter_id': '95', 'district': '23', 'party': 'R'},
 {'name': 'Time Erwin', 'voter_id': '127', 'district': '44', 'party': 'R'},
 {'name': 'Marcus Gaspard', 'voter_id': '148', 'district': '25', 'party': 'D'},
 {'name': 'Frank Hansen', 'voter_id': '168', 'district': '13', 'party': 'D'},
 {'name': 'Jeannette Hayner', 'voter_id': '176', 'district': '16', 'party': 'R'},
 {'name': 'Jim Jesernig', 'voter_id': '211', 'district': '8', 'party': 'D'},
 {'name': 'Lela Kreidler,', 'voter_id': '232', 'district': '22', 'party': 'D'},
 {'name': 'Ken Madsen', 'voter_id': '256', 'district': '2', 'party': 'D'},
 {'name': 'Jim Matson', 'voter_id': '261', 'district': '14', 'party': 'R'},
 {'name': 'Patrick McMullen', 'voter_id': '278', 'district': '40', 'party': 'D'},
 {'name': 'Jack Metcalf', 'voter_id': '281', 'district': '10', 'party': 'R'},
 {'name': 'Ray Moore', 'voter_id': '290', 'district': '36', 'party': 'D'},
 {'name': 'Patty Murray', 'voter_id': '299', 'district': '1', 'party': 'D'},
 {'name': 'Gary Nelson', 'voter_id': '308', 'district': '21', 'party': 'R'},
 {'name': 'Janice Niemi', 'voter_id': '310', 'district': '43', 'party': 'D'},
 {'name': 'E-G Patterson', 'voter_id': '326', 'district': '9', 'party': 'R'},
 {'name': 'Dwight Pelz', 'voter_id': '329', 'district': '37', 'party': 'D'},
 {'name': 'A-L Rasmussen', 'voter_id': '345', 'district': '29', 'party': 'D'},
 {'name': 'Nina Rinehart', 'voter_id': '360', 'district': '46', 'party': 'D'},
 {'name': 'Gerald Saling', 'voter_id': '370', 'district': '5', 'party': 'R'},
 {'name': 'George Sellar', 'voter_id': '387', 'district': '12', 'party': 'R'},
 {'name': 'Sylvia Skratek', 'voter_id': '399', 'district': '47', 'party': 'D'},
 {'name': 'Adam Smith', 'voter_id': '402', 'district': '33', 'party': 'D'},
 {'name': 'Linda Smith', 'voter_id': '403', 'district': '18', 'party': 'R'},
 {'name': 'Lois Stratton', 'voter_id': '414', 'district': '3', 'party': 'D'},
 {'name': 'Dean Sutherland', 'voter_id': '423', 'district': '17', 'party': 'D'},
 {'name': 'Phil Talmadge', 'voter_id': '428', 'district': '34', 'party': 'D'},
 {'name': 'Leo Thorsness', 'voter_id': '434', 'district': '11', 'party': 'R'},
 {'name': 'Larry Vognild', 'voter_id': '445', 'district': '38', 'party': 'D'},
 {'name': 'Peter von-Reichbauer', 'voter_id': '446', 'district': '30', 'party': 'R'},
 {'name': 'Alvin Williams', 'voter_id': '467', 'district': '32', 'party': 'D'},
 {'name': 'R-Lorraine Wojahn', 'voter_id': '477', 'district': '27', 'party': 'D'},
 {'name': 'Myron Kreidler', 'voter_id': '233', 'district': '22', 'party': 'D'},
 {'name': 'Wanda Hansen', 'voter_id': '169', 'district': '13', 'party': 'D'},
 {'name': 'Susan Sumner', 'voter_id': '419', 'district': '28', 'party': 'R'},
 {'name': 'Barbara Holm', 'voter_id': '639', 'district': '35', 'party': 'D'},
 {'name': 'Kathleen Drew', 'voter_id': '570', 'district': '5', 'party': 'D'},
 {'name': 'Valoria Loveland', 'voter_id': '562', 'district': '16', 'party': 'D'},
 {'name': 'Kevin Quigley', 'voter_id': '563', 'district': '39', 'party': 'D'},
 {'name': 'David Chappell', 'voter_id': '621', 'district': '20', 'party': 'D'},
 {'name': 'Barbara Cothern', 'voter_id': '629', 'district': '1', 'party': 'D'},
 {'name': 'Stanley Flemming', 'voter_id': '636', 'district': '28', 'party': 'D'},
 {'name': 'Dale Foreman', 'voter_id': '637', 'district': '12', 'party': 'R'},
 {'name': 'Mick Hansen', 'voter_id': '638', 'district': '13', 'party': 'D'},
 {'name': 'Jim Johanson', 'voter_id': '640', 'district': '44', 'party': 'D'},
 {'name': 'Linda Johnson', 'voter_id': '641', 'district': '1', 'party': 'D'},
 {'name': 'Sue Karahalios', 'voter_id': '642', 'district': '10', 'party': 'D'},
 {'name': 'Dave Lemmon', 'voter_id': '645', 'district': '14', 'party': 'D'},
 {'name': 'Jim Springer', 'voter_id': '656', 'district': '18', 'party': 'D'},
 {'name': 'Brian Thomas', 'voter_id': '660', 'district': '5', 'party': 'R'},
 {'name': 'Cathy Wolfe', 'voter_id': '662', 'district': '22', 'party': 'D'},
 {'name': 'Bill Backlund', 'voter_id': '1369', 'district': '45', 'party': 'R'},
 {'name': 'Vivian Caver', 'voter_id': '1408', 'district': '37', 'party': 'D'},
 {'name': 'Thomas Moak', 'voter_id': '1351', 'district': '8', 'party': 'D'},
 {'name': 'Ray Schow', 'voter_id': '1352', 'district': '30', 'party': 'R'},
 {'name': 'Barney Beeksma', 'voter_id': '1582', 'district': '10', 'party': 'R'},
 {'name': 'Jerry Blanton', 'voter_id': '1569', 'district': '21', 'party': 'R'},
 {'name': 'Gene Goldsmith', 'voter_id': '1575', 'district': '42', 'party': 'R'},
 {'name': 'Steve Hargrove', 'voter_id': '1588', 'district': '23', 'party': 'R'},
 {'name': 'Tomm Huff', 'voter_id': '1578', 'district': '26', 'party': 'R'},
 {'name': 'Kathy Lambert', 'voter_id': '1589', 'district': '45', 'party': 'R'},
 {'name': 'John Pennington', 'voter_id': '1591', 'district': '18', 'party': 'R'},
 {'name': 'Renee Radcliff', 'voter_id': '1579', 'district': '21', 'party': 'R'},
 {'name': 'Mike Sherstad', 'voter_id': '1593', 'district': '1', 'party': 'R'},
 {'name': 'Darlene Fairley', 'voter_id': '1603', 'district': '32', 'party': 'D'},
 {'name': 'Hal Palmer', 'voter_id': '1667', 'district': '18', 'party': 'R'},
 {'name': 'Calvin Goings', 'voter_id': '1938', 'district': '25', 'party': 'D'},
 {'name': 'Carl Scheuerman', 'voter_id': '1949', 'district': '29', 'party': 'D'},
 {'name': 'Brian Sullivan', 'voter_id': '2132', 'district': '29', 'party': 'D'},
 {'name': 'Mike Wensman', 'voter_id': '2222', 'district': '41', 'party': 'R'},
 {'name': 'Lena Swanson', 'voter_id': '2202', 'district': '35', 'party': 'D'},
 {'name': 'Glenn Anderson', 'voter_id': '2147', 'district': '5', 'party': 'R'},
 {'name': 'John Blalock', 'voter_id': '2296', 'district': '33', 'party': 'D'},
 {'name': 'Carolyn Edmonds', 'voter_id': '3471', 'district': '32', 'party': 'D'},
 {'name': 'Michael Stensen', 'voter_id': '3485', 'district': '31', 'party': 'D'},
 {'name': 'Joe Marine', 'voter_id': '6307', 'district': '21', 'party': 'R'},
 {'name': 'John Wyss', 'voter_id': '11176', 'district': '6', 'party': 'R'},
 {'name': 'Eric Oemig', 'voter_id': '12067', 'district': '45', 'party': 'D'},
 {'name': 'Gina McCabe', 'voter_id': '20759', 'district': '14', 'party': 'R'},
 {'name': 'William Jenkin', 'voter_id': '26172', 'district': '16', 'party': 'R'},
 {'name': 'Keith Wagoner', 'voter_id': '28317', 'district': '39', 'party': 'R'},
 {'name': 'Stanley Johnson', 'voter_id': '420', 'district': '28', 'party':'R'},
 {'name': 'Ron Carlson', 'voter_id': '877', 'district': '47', 'party':'R'},
 {'name': 'Georgette Valle', 'voter_id': '442', 'district': '34', 'party':'D'},
 {'name': 'Richard Neher', 'voter_id': '305', 'district': '16', 'party':'R'}, 
 {'name': 'Richard Nelson', 'voter_id': '307', 'district': '32', 'party':'D'}]

 missing_legislators_df = pd.DataFrame(missing_legislators)
 missing_legislators_df['first_name'] = missing_legislators_df['name'].apply(lambda x: x.split()[0])
 missing_legislators_df['last_name'] = missing_legislators_df['name'].apply(lambda x: x.split()[1])
 missing_legislators_df = missing_legislators_df.drop('name', axis=1)
 manual_df.to_csv('missing_legislators.csv', sep='|', index=False)
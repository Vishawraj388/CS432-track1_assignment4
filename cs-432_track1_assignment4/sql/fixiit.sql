DROP DATABASE IF EXISTS fixiit;
CREATE DATABASE fixiit;
USE fixiit;

CREATE TABLE role (
    role_id INT PRIMARY KEY AUTO_INCREMENT,
    role_name VARCHAR(20) NOT NULL UNIQUE
);

INSERT INTO role (role_name) VALUES
('user'), ('staff'), ('admin');

CREATE TABLE issue_type (
    issue_type_id INT PRIMARY KEY AUTO_INCREMENT,
    issue_name VARCHAR(30) NOT NULL UNIQUE
);

INSERT INTO issue_type (issue_name) VALUES
('electrical'), ('plumbing'), ('internet'), ('furniture'), ('cleaning');

CREATE TABLE priority (
    priority_id INT PRIMARY KEY AUTO_INCREMENT,
    priority_level VARCHAR(20) NOT NULL UNIQUE
);

INSERT INTO priority (priority_level) VALUES
('low'), ('medium'), ('high'), ('critical');

CREATE TABLE status (
    status_id INT PRIMARY KEY AUTO_INCREMENT,
    status_name VARCHAR(20) NOT NULL UNIQUE
);

INSERT INTO status (status_name) VALUES
('open'), ('assigned'), ('in progress'), ('resolved'), ('closed');

CREATE TABLE hostel (
    hostel_id INT PRIMARY KEY AUTO_INCREMENT,
    hostel_name VARCHAR(50) NOT NULL UNIQUE
);

INSERT INTO hostel (hostel_name) VALUES
('Aibaan'), ('Beauki'), ('Chimair'), ('Duven'),
('Emiet'), ('Firpeal'), ('Griwiksh'),
('Hiqom'), ('Ijokha'), ('Jurqia'),
('Kyzeel'), ('Lekhaag');

CREATE TABLE location (
    location_id INT PRIMARY KEY AUTO_INCREMENT,
    location_name VARCHAR(100) NOT NULL UNIQUE,
    block_room_pattern VARCHAR(20)
);

INSERT INTO location (location_name) VALUES
('Academic Block'),
('Central Arcade'),
('Research Park'),
('Guest House'),
('Housing Block'),
('Sports Complex'),
('Infrastructure Area');

CREATE TABLE member (
    member_id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL UNIQUE,
    name VARCHAR(50) NOT NULL,
    contact_number VARCHAR(15) NOT NULL,
    role_id INT NOT NULL,
    hostel_id INT,
    hostel_room_no VARCHAR(20),
    location_id INT,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (role_id) REFERENCES role(role_id),
    FOREIGN KEY (hostel_id) REFERENCES hostel(hostel_id),
    FOREIGN KEY (location_id) REFERENCES location(location_id),
    CHECK (
        (hostel_id IS NOT NULL AND hostel_room_no IS NOT NULL)
        OR
        (location_id IS NOT NULL)
    )
);

CREATE TABLE complaint (
    complaint_id INT PRIMARY KEY AUTO_INCREMENT,
    member_id INT NOT NULL,
    issue_type_id INT NOT NULL,
    priority_id INT NOT NULL,
    status_id INT NOT NULL,
    description TEXT NOT NULL,
    created_at DATETIME NOT NULL,
    closed_at DATETIME,
    hostel_id INT,
    hostel_room_no VARCHAR(20),
    location_id INT,
    CHECK (closed_at IS NULL OR closed_at > created_at),
    FOREIGN KEY (member_id) REFERENCES member(member_id),
    FOREIGN KEY (issue_type_id) REFERENCES issue_type(issue_type_id),
    FOREIGN KEY (priority_id) REFERENCES priority(priority_id),
    FOREIGN KEY (status_id) REFERENCES status(status_id),
    FOREIGN KEY (hostel_id) REFERENCES hostel(hostel_id),
    FOREIGN KEY (location_id) REFERENCES location(location_id),
    CHECK (
        (hostel_id IS NOT NULL AND hostel_room_no IS NOT NULL)
        OR
        (location_id IS NOT NULL)
    )
);

CREATE TABLE custom_issue (
    custom_issue_id INT PRIMARY KEY AUTO_INCREMENT,
    complaint_id INT NOT NULL,
    problem_title VARCHAR(100) NOT NULL,
    problem_description TEXT NOT NULL,
    FOREIGN KEY (complaint_id) REFERENCES complaint(complaint_id)
);

CREATE TABLE assignment (
    assignment_id INT PRIMARY KEY AUTO_INCREMENT,
    complaint_id INT NOT NULL,
    admin_id INT NOT NULL,
    assigned_at DATETIME NOT NULL,
    FOREIGN KEY (complaint_id) REFERENCES complaint(complaint_id),
    FOREIGN KEY (admin_id) REFERENCES member(member_id)
);

CREATE TABLE technician_availability (
    technician_id INT PRIMARY KEY,
    availability_status VARCHAR(20) NOT NULL CHECK (availability_status IN ('available', 'busy')),
    FOREIGN KEY (technician_id) REFERENCES member(member_id)
);

CREATE TABLE maintenance_log (
    log_id INT PRIMARY KEY AUTO_INCREMENT,
    complaint_id INT NOT NULL,
    updated_by INT NOT NULL,
    log_message TEXT NOT NULL,
    log_time DATETIME NOT NULL,
    FOREIGN KEY (complaint_id) REFERENCES complaint(complaint_id),
    FOREIGN KEY (updated_by) REFERENCES member(member_id)
);

CREATE TABLE feedback (
    feedback_id INT PRIMARY KEY AUTO_INCREMENT,
    complaint_id INT NOT NULL UNIQUE,
    rating INT NOT NULL CHECK (rating BETWEEN 1 AND 5),
    comments TEXT,
    feedback_date DATETIME NOT NULL,
    FOREIGN KEY (complaint_id) REFERENCES complaint(complaint_id)
);

CREATE TABLE notification (
    notification_id INT PRIMARY KEY AUTO_INCREMENT,
    member_id INT NOT NULL,
    message TEXT NOT NULL,
    sent_at DATETIME NOT NULL,
    FOREIGN KEY (member_id) REFERENCES member(member_id)
);

INSERT INTO users (username, email, password, role) VALUES
('rahul', 'rahul@iitgn.ac.in', 'fixiit123', 'Regular User'),
('ananya', 'ananya@iitgn.ac.in', 'fixiit123', 'Regular User'),
('kunal', 'kunal@iitgn.ac.in', 'fixiit123', 'Regular User'),
('sneha', 'sneha@iitgn.ac.in', 'fixiit123', 'Regular User'),
('amit', 'amit@iitgn.ac.in', 'fixiit123', 'Regular User'),
('arjun', 'arjun@iitgn.ac.in', 'fixiit123', 'Regular User'),
('meera', 'meera@iitgn.ac.in', 'fixiit123', 'Regular User'),
('dev', 'dev@iitgn.ac.in', 'fixiit123', 'Regular User'),
('isha', 'isha@iitgn.ac.in', 'fixiit123', 'Regular User'),
('riya', 'riya@iitgn.ac.in', 'fixiit123', 'Regular User'),
('rohit.staff', 'rohit.staff@iitgn.ac.in', 'fixiit123', 'Staff'),
('neha.staff', 'neha.staff@iitgn.ac.in', 'fixiit123', 'Staff'),
('vikas.staff', 'vikas.staff@iitgn.ac.in', 'fixiit123', 'Staff'),
('admin1', 'admin1@iitgn.ac.in', 'fixiit123', 'Admin'),
('admin2', 'admin2@iitgn.ac.in', 'fixiit123', 'Admin'),
('admin', 'admin@fixiit.local', 'admin123', 'Admin'),
('kushal', 'kushal@fixiit.local', 'kushal123', 'Regular User');

INSERT INTO member (user_id, name, contact_number, role_id, hostel_id, hostel_room_no) VALUES
(1, 'Rahul Sharma', '9000000001', 1, 1, '101'),
(2, 'Ananya Patel', '9000000002', 1, 2, '202'),
(3, 'Kunal Mehta', '9000000003', 1, 3, '305'),
(4, 'Sneha Iyer', '9000000004', 1, 4, '412'),
(5, 'Amit Verma', '9000000005', 1, 5, '118'),
(6, 'Arjun Nair', '9000000031', 1, 6, '210'),
(7, 'Meera Shah', '9000000032', 1, 7, '315'),
(8, 'Dev Patel', '9000000033', 1, 8, '412'),
(9, 'Isha Rao', '9000000034', 1, 9, '109'),
(10, 'Riya Das', '9000000035', 1, 10, '205');

INSERT INTO member (user_id, name, contact_number, role_id, location_id) VALUES
(11, 'Rohit Singh', '9000000011', 2, 1),
(12, 'Neha Joshi', '9000000012', 2, 2),
(13, 'Vikas Kumar', '9000000013', 2, 4),
(14, 'Admin One', '9000000021', 3, 4),
(15, 'Admin Two', '9000000022', 3, 5),
(16, 'System Admin', '0000000000', 3, 1),
(17, 'Kushal', '9000000099', 1, 1);

INSERT INTO complaint VALUES
(1, 1, 1, 3, 1, 'Fan not working', '2026-01-05 09:15:00', '2026-01-06 10:30:00', 1, '101', NULL),
(2, 2, 2, 2, 2, 'Water leakage', '2026-01-06 11:00:00', '2026-01-07 13:45:00', 2, '202', NULL),
(3, 3, 3, 2, 3, 'WiFi issue', '2026-01-07 14:20:00', '2026-01-09 09:30:00', 3, '305', NULL),
(4, 4, 5, 1, 4, 'Cleaning required', '2026-01-08 08:10:00', '2026-01-08 17:00:00', 4, '412', NULL),
(5, 5, 4, 3, 2, 'Broken hinge', '2026-01-09 10:00:00', '2026-01-10 12:00:00', 5, '118', NULL),
(6, 6, 1, 2, 1, 'Tube light flicker', '2026-01-10 09:45:00', '2026-01-11 10:15:00', 6, '210', NULL),
(7, 7, 2, 3, 2, 'Pipe leakage', '2026-01-11 13:00:00', '2026-01-12 14:20:00', 7, '315', NULL),
(8, 8, 3, 2, 1, 'Slow WiFi', '2026-01-12 15:00:00', '2026-01-13 16:00:00', 8, '412', NULL),
(9, 9, 5, 1, 3, 'Dust in corridor', '2026-01-13 09:00:00', '2026-01-14 10:30:00', 9, '109', NULL),
(10, 10, 4, 3, 2, 'Broken table', '2026-01-14 11:15:00', '2026-01-15 12:45:00', 10, '205', NULL),
(11, 11, 1, 4, 2, 'Power outage', '2026-01-15 10:00:00', '2026-01-16 11:30:00', NULL, NULL, 1),
(12, 12, 3, 2, 1, 'Network down', '2026-01-16 09:30:00', '2026-01-17 10:45:00', NULL, NULL, 2),
(13, 13, 5, 1, 2, 'Area cleaning', '2026-01-17 14:10:00', '2026-01-18 15:00:00', NULL, NULL, 3),
(14, 14, 4, 2, 3, 'Broken chair', '2026-01-18 08:40:00', '2026-01-19 09:20:00', NULL, NULL, 4),
(15, 15, 2, 3, 1, 'Water blockage', '2026-01-19 12:00:00', '2026-01-20 13:15:00', NULL, NULL, 5);

INSERT INTO custom_issue (complaint_id, problem_title, problem_description) VALUES
(1, 'Fan noise', 'Fan making loud noise'),
(2, 'Bathroom leakage', 'Water dripping from ceiling'),
(3, 'WiFi unstable', 'Frequent disconnections'),
(4, 'Room dusty', 'Needs cleaning urgently'),
(5, 'Hinge broken', 'Cupboard hinge snapped'),
(6, 'Light flicker', 'Tube light blinking'),
(7, 'Pipe burst', 'Water spreading'),
(8, 'Internet slow', 'Low speed'),
(9, 'Trash issue', 'Garbage not cleared'),
(10, 'Table damage', 'Wood broken'),
(11, 'Switch burnt', 'Short circuit smell'),
(12, 'Router issue', 'Router not responding'),
(13, 'Hygiene issue', 'Common area dirty'),
(14, 'Chair damage', 'One leg broken'),
(15, 'Drain blocked', 'Water not draining');

INSERT INTO technician_availability VALUES
(11, 'busy'),
(12, 'available'),
(13, 'busy');

INSERT INTO assignment (complaint_id, admin_id, assigned_at) VALUES
(1, 14, '2026-01-05 09:45:00'),
(2, 14, '2026-01-06 11:30:00'),
(3, 15, '2026-01-07 14:45:00'),
(4, 14, '2026-01-08 08:30:00'),
(5, 15, '2026-01-09 10:20:00'),
(6, 14, '2026-01-10 10:00:00'),
(7, 15, '2026-01-11 13:20:00'),
(8, 14, '2026-01-12 15:30:00'),
(9, 15, '2026-01-13 09:25:00'),
(10, 14, '2026-01-14 11:40:00'),
(11, 15, '2026-01-15 10:30:00'),
(12, 14, '2026-01-16 09:50:00'),
(13, 15, '2026-01-17 14:30:00'),
(14, 14, '2026-01-18 09:00:00'),
(15, 15, '2026-01-19 12:30:00');

INSERT INTO maintenance_log (complaint_id, updated_by, log_message, log_time) VALUES
(1, 11, 'Electrician assigned', '2026-01-05 10:15:00'),
(1, 11, 'Fan repaired', '2026-01-06 10:20:00'),
(2, 12, 'Plumber inspected', '2026-01-06 12:00:00'),
(2, 12, 'Leak fixed', '2026-01-07 13:30:00'),
(3, 13, 'Network diagnostics started', '2026-01-07 15:00:00'),
(3, 13, 'WiFi stabilized', '2026-01-09 09:00:00'),
(4, 12, 'Cleaning staff dispatched', '2026-01-08 09:00:00'),
(5, 11, 'Carpenter assigned', '2026-01-09 11:00:00'),
(6, 11, 'Tube light replaced', '2026-01-11 09:50:00'),
(7, 12, 'Pipe replaced', '2026-01-12 14:00:00'),
(8, 13, 'Router rebooted', '2026-01-13 15:40:00'),
(9, 12, 'Corridor cleaned', '2026-01-14 10:00:00'),
(10, 11, 'Table repaired', '2026-01-15 12:00:00'),
(11, 11, 'Switch rewired', '2026-01-16 11:00:00'),
(12, 13, 'Router replaced', '2026-01-17 10:00:00'),
(13, 12, 'Area sanitized', '2026-01-18 15:10:00'),
(14, 11, 'Chair fixed', '2026-01-19 09:10:00'),
(15, 12, 'Drain cleared', '2026-01-20 13:00:00');

INSERT INTO feedback (complaint_id, rating, comments, feedback_date) VALUES
(1, 5, 'Resolved quickly', '2026-01-06 12:00:00'),
(2, 4, 'Issue fixed properly', '2026-01-07 15:00:00'),
(3, 3, 'Took some time', '2026-01-09 11:00:00'),
(4, 5, 'Very clean now', '2026-01-08 18:00:00'),
(5, 4, 'Satisfied with service', '2026-01-10 14:00:00'),
(6, 5, 'Quick replacement', '2026-01-11 11:00:00'),
(7, 4, 'Leak solved', '2026-01-12 16:00:00'),
(8, 4, 'Internet stable now', '2026-01-13 17:00:00'),
(9, 5, 'Good cleaning', '2026-01-14 12:00:00'),
(10, 4, 'Furniture fixed well', '2026-01-15 14:00:00');

INSERT INTO notification (member_id, message, sent_at) VALUES
(1, 'Your complaint has been assigned', '2026-01-05 09:50:00'),
(2, 'Technician will visit today', '2026-01-06 11:40:00'),
(3, 'Issue under review', '2026-01-07 14:50:00'),
(4, 'Cleaning team dispatched', '2026-01-08 08:45:00'),
(5, 'Repair work started', '2026-01-09 10:30:00'),
(6, 'Complaint assigned to technician', '2026-01-10 10:05:00'),
(7, 'Maintenance scheduled', '2026-01-11 13:25:00'),
(8, 'Network team notified', '2026-01-12 15:35:00'),
(9, 'Cleaning scheduled', '2026-01-13 09:30:00'),
(10, 'Carpenter assigned', '2026-01-14 11:45:00'),
(11, 'Power issue being handled', '2026-01-15 10:40:00'),
(12, 'Network technician assigned', '2026-01-16 09:55:00'),
(13, 'Area cleaning in progress', '2026-01-17 14:35:00'),
(14, 'Furniture repair underway', '2026-01-18 09:05:00'),
(15, 'Plumber dispatched', '2026-01-19 12:35:00');

CREATE INDEX idx_member_role_id ON member (role_id);
CREATE INDEX idx_complaint_member_id ON complaint (member_id);
CREATE INDEX idx_complaint_status_id ON complaint (status_id);
CREATE INDEX idx_complaint_created_at ON complaint (created_at DESC);

-- Assignment 4 sharding: complaints are partitioned by member_id % 3.
CREATE TABLE complaint_shard_map (
    complaint_id INT PRIMARY KEY,
    member_id INT NOT NULL,
    shard_id INT NOT NULL CHECK (shard_id >= 0 AND shard_id < 3)
);

CREATE TABLE shard_0_complaint LIKE complaint;
CREATE TABLE shard_1_complaint LIKE complaint;
CREATE TABLE shard_2_complaint LIKE complaint;

INSERT INTO shard_0_complaint
SELECT * FROM complaint WHERE MOD(member_id, 3) = 0;

INSERT INTO shard_1_complaint
SELECT * FROM complaint WHERE MOD(member_id, 3) = 1;

INSERT INTO shard_2_complaint
SELECT * FROM complaint WHERE MOD(member_id, 3) = 2;

INSERT INTO complaint_shard_map (complaint_id, member_id, shard_id)
SELECT complaint_id, member_id, MOD(member_id, 3)
FROM complaint;

CREATE INDEX idx_complaint_shard_map_member_id ON complaint_shard_map (member_id);
CREATE INDEX idx_complaint_shard_map_shard_id ON complaint_shard_map (shard_id);
CREATE INDEX idx_shard_0_complaint_member_id ON shard_0_complaint (member_id);
CREATE INDEX idx_shard_1_complaint_member_id ON shard_1_complaint (member_id);
CREATE INDEX idx_shard_2_complaint_member_id ON shard_2_complaint (member_id);
CREATE INDEX idx_shard_0_complaint_created_at ON shard_0_complaint (created_at DESC);
CREATE INDEX idx_shard_1_complaint_created_at ON shard_1_complaint (created_at DESC);
CREATE INDEX idx_shard_2_complaint_created_at ON shard_2_complaint (created_at DESC);

CREATE TRIGGER trg_member_insert
AFTER INSERT ON member
BEGIN
    INSERT INTO logs (actor, source, table_name, action, details)
    SELECT actor, source, 'member', 'INSERT', 'member_id=' || NEW.member_id || ', user_id=' || NEW.user_id
    FROM audit_context
    WHERE id = 1;
END;

CREATE TRIGGER trg_member_update
AFTER UPDATE ON member
BEGIN
    INSERT INTO logs (actor, source, table_name, action, details)
    SELECT actor, source, 'member', 'UPDATE', 'member_id=' || NEW.member_id || ', user_id=' || NEW.user_id
    FROM audit_context
    WHERE id = 1;
END;

CREATE TRIGGER trg_member_delete
AFTER DELETE ON member
BEGIN
    INSERT INTO logs (actor, source, table_name, action, details)
    SELECT actor, source, 'member', 'DELETE', 'member_id=' || OLD.member_id || ', user_id=' || OLD.user_id
    FROM audit_context
    WHERE id = 1;
END;

CREATE TRIGGER trg_complaint_insert
AFTER INSERT ON complaint
BEGIN
    INSERT INTO logs (actor, source, table_name, action, details)
    SELECT actor, source, 'complaint', 'INSERT', 'complaint_id=' || NEW.complaint_id || ', member_id=' || NEW.member_id
    FROM audit_context
    WHERE id = 1;
END;

CREATE TRIGGER trg_complaint_update
AFTER UPDATE ON complaint
BEGIN
    INSERT INTO logs (actor, source, table_name, action, details)
    SELECT actor, source, 'complaint', 'UPDATE', 'complaint_id=' || NEW.complaint_id || ', member_id=' || NEW.member_id
    FROM audit_context
    WHERE id = 1;
END;

CREATE TRIGGER trg_complaint_delete
AFTER DELETE ON complaint
BEGIN
    INSERT INTO logs (actor, source, table_name, action, details)
    SELECT actor, source, 'complaint', 'DELETE', 'complaint_id=' || OLD.complaint_id || ', member_id=' || OLD.member_id
    FROM audit_context
    WHERE id = 1;
END;

SELECT * FROM role;
SELECT * FROM issue_type;
SELECT * FROM priority;
SELECT * FROM status;
SELECT * FROM hostel;
SELECT * FROM location;
SELECT * FROM member;
SELECT * FROM complaint;
SELECT * FROM custom_issue;
SELECT * FROM assignment;
SELECT * FROM technician_availability;
SELECT * FROM maintenance_log;
SELECT * FROM feedback;
SELECT * FROM notification;

CREATE TABLE tbl_TDSMenu (
  menu_id INT PRIMARY KEY,
  menu_name VARCHAR(255) NOT NULL,
  parent_menu_id INT,
  menu_order INT,
  menu_url VARCHAR(255),
  is_active BIT DEFAULT 1,
  created_at DATETIME DEFAULT GETDATE(),
  updated_at DATETIME
);
--drop table tbl_TDSMenu

INSERT INTO tbl_TDSMenu (menu_id, menu_name, parent_menu_id, menu_order, menu_url)
VALUES (1, 'Home', NULL, 1, '/home');

-- Insert a submenu item
INSERT INTO tbl_TDSMenu (menu_id, menu_name, parent_menu_id, menu_order, menu_url)
VALUES (2, 'Reports', 1, 2, '/TDSReportgeneration/Index');

INSERT INTO tbl_TDSMenu (menu_id, menu_name, parent_menu_id, menu_order, menu_url)
VALUES (3, 'Recomputation', 1, 2, '/Recomputation/Index');
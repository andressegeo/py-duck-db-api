SELECT `company`.`id` AS `company.id`, `company.contact`.`id` AS `company.contact.id`, `company.contact`.`number` AS `company.contact.number` FROM `company`
JOIN `phone`
AS `company.contact`
ON `company`.`contact` = `company.contact`.`id`

JOIN `type`
AS `company.contact.type`
ON `company.contact`.`type` = `company.contact.type`.`id`

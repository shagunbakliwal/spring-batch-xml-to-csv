package com.spring.batch.processor;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Service;

import com.spring.batch.model.Report;

@Service
public class FilterReportProcessor implements ItemProcessor<Report, Report> {

	@Override
	public Report process(Report item) throws Exception {
		if (item.getAge() == 30) {
			return null;
		}
		return item;
	}

}

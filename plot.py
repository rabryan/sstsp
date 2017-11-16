from collections import OrderedDict
import os.path
from bokeh.plotting import figure 
from bokeh.models.sources import ColumnDataSource, AjaxDataSource
from bokeh.models import BoxSelectTool, HoverTool
from bokeh.models.callbacks import CustomJS
import pandas as pd
import logging as log

import requests
import sstsp


def style_axis(plot, theme):
    plot.axis.minor_tick_in=None
    plot.axis.minor_tick_out=None
    plot.axis.major_tick_in=None
    plot.axis.major_label_text_font_size="10pt"
    plot.axis.major_label_text_font_style="normal"
    plot.axis.axis_label_text_font_size="10pt"
    plot.axis.axis_line_width=1
    plot.axis.major_tick_line_width=1


    plot.axis.major_tick_line_cap="round"
    plot.axis.axis_line_cap="round"

    if theme == 'default':
        plot.axis.axis_line_color='#AAAAAA'
        plot.axis.major_tick_line_color='#AAAAAA'
        plot.axis.major_label_text_color='#666666'

    elif theme == 'dark':
        plot.axis.axis_line_color='#808080'
        plot.axis.major_tick_line_color='#808080'
        plot.axis.major_label_text_color='#666666'
        plot.outline_line_color = "#E6E6E6"
        plot.outline_line_alpha = 0


def style_selection_plot(selection_plot, theme='default'):
    style_axis(selection_plot, theme)
    selection_plot.min_border_bottom = selection_plot.min_border_top = 0
    selection_plot.ygrid.grid_line_color = None


    if theme == 'default':
        selection_plot.background_fill_color = "white"
        selection_plot.border_fill_color = "white"
        selection_plot.yaxis.major_label_text_color = "white"
        selection_plot.yaxis.minor_tick_line_color="white"
        selection_plot.yaxis.major_tick_line_color=None

    elif theme == 'dark':
        selection_plot.background_fill_color = "#333333"
        selection_plot.border_fill_color = "#191919"
        selection_plot.yaxis.major_label_text_color = "#191919"
        selection_plot.yaxis.minor_tick_line_color="#191919"
        selection_plot.yaxis.major_tick_line_color=None


def style_main_plot(p, theme='default'):
    style_axis(p, theme)

    if theme == 'default':
        p.background_fill_color = "white"
        p.border_fill_color = "white"

    elif theme == 'dark':
        p.background_fill_color = "#333333"
        p.border_fill_color = "#191919"
        p.grid.grid_line_color = "#4D4D4D"



def create_main_plot(theme, source):
    p = figure(x_axis_type = "datetime", tools="pan,xwheel_zoom,ywheel_zoom,box_zoom,reset,previewsave",
               height=500, toolbar_location='right', active_scroll='xwheel_zoom',
               responsive=True)
    p.line('index', 'data', color='#A6CEE3', source=source)
    p.circle('index', 'data', color='#A6CEE3', source=source, size=2)
    style_main_plot(p, theme)

#    hover = p.select(dict(type=HoverTool))
#    hover.mode='vline'
#    hover.tooltips = OrderedDict([
#     ("Date", "@DateFmt"),
#     ("Value", "@data"),
#    ])
    return p

def create_selection_plot(main_plot, theme):

    static_source = ColumnDataSource(data)
    selection_plot = figure(
        height=100, tools="box_select", x_axis_location="above",
        x_axis_type="datetime", toolbar_location=None,
        outline_line_color=None, name="small_plot"
    )
    selection_source = ColumnDataSource()
    for k in ['end', 'values', 'start', 'bottom']:
        selection_source.add([], k)

    if theme == 'default':
        selection_color = '#c6dbef'
    elif theme == 'dark':
        selection_color = "#FFAD5C"

    selection_plot.quad(top='values', bottom='bottom', left='start', right='end',
          source=selection_source, color=selection_color, fill_alpha=0.5)

    selection_plot.line('index', 'data', color='#A6CEE3', source=static_source)
    selection_plot.circle('index', 'data', color='#A6CEE3', source=static_source, size=1)

    style_selection_plot(selection_plot, theme)

    select_tool = selection_plot.select(dict(type=BoxSelectTool))
    select_tool.dimensions = ['width']

    code = """
        if (window.xrange_base_start == undefined){
            window.xrange_base_start = main_plot.get('x_range').get('start');
        }
        if (window.xrange_base_end == undefined){
            window.xrange_base_end = main_plot.get('x_range').get('end');
        }

        data = source.get('data');
        sel = source.get('selected')['1d']['indices'];
        var mi = 1000000000;
        var ma = -100000;
        if (sel.length == 0){
           var url = "http://127.0.0.1:5000/alldata";
           source_data = selection_source.get('data');
           source_data.bottom = []
           source_data.values = [];
           source_data.start = [];
           source_data.end = [];

            // reset main plot ranges
            main_range.set('start', window.xrange_base_start);
            main_range.set('end', window.xrange_base_end);
        }else{
           for (i=0; i<sel.length; i++){
            if (mi>sel[i]){
                mi = sel[i];
            }
            if (ma<sel[i]){
                ma = sel[i];
            }
           }
           var url = "http://127.0.0.1:5000/subsample/"+data.Date[mi]+"/"+data.Date[ma];
           source_data = selection_source.get('data');
           source_data.bottom = [0]
           source_data.values = [700];
           source_data.start = [data.Date[mi]];
           source_data.end = [data.Date[ma]];

           main_range = main_plot.get('x_range');
           main_range.set('start', data.Date[mi]);
           main_range.set('end', data.Date[ma]);
        }

        xmlhttp = new XMLHttpRequest();
        xmlhttp.open("GET", url, true);
        xmlhttp.send();

        selection_source.trigger('change');

        if (sel.length==0){
            $("#details_panel").addClass("hidden");
            $("#details_panel").html("");
        }else{

            var url = "http://127.0.0.1:5000/details";
            xhr = $.ajax({
                type: 'GET',
                url: url,
                contentType: "application/json",
                // data: jsondata,
                header: {
                  client: "javascript"
                }
            });

            xhr.done(function(details) {
                $("#details_panel").removeClass("hidden");
                $("#details_panel").html("<h3>Selected Region Report</h3>");
                $("#details_panel").append("<div>From " + details.start + " to " + details.end + "</div>");
                $("#details_panel").append("<div>Number of original samples " + details.original_samples_no + "</div>");
                $("#details_panel").append("<div>Number of samples " + details.samples_no + "</div>");
                $("#details_panel").append("<div>Factor " + details.factor + "</div>");
            });
        }

    """

    callback = CustomJS(
           args={'source': static_source,
                 'selection_source': selection_source,
                 'main_plot': main_plot},
           code=code)
    static_source.callback = callback

    return selection_plot


# Create the flask app to serve the customized panel
from flask import Flask, render_template, jsonify, request, abort

app = Flask('sstsp')



    

def get_ajax_latest_source(user_id, data_id):
    data_url = "http://127.0.0.1:5000/d/{}/{}".format(user_id, data_id)
    latest_url = data_url + "/latest"
    latest_src =  AjaxDataSource(dict(index=[], data=[]), data_url=latest_url, 
            polling_interval=5000, method='GET')

    def on_latest_change(attr, old, new):
        print('on_latest_change {}  old={} new={}'.format(attr, old, new))

    latest_src.on_change('data', on_latest_change)

    return latest_src


gen_config = dict(
    applet_url="http://127.0.0.1:5050",
    host='0.0.0.0',
    port=5050,
    debug=True,
)
if __name__ == "__main__":
    print("\nView this example at: %s\n" % gen_config['applet_url'])
    log.basicConfig(level=log.DEBUG)
    app.debug = gen_config['debug']
    app.run(host=gen_config['host'], port=gen_config['port'])

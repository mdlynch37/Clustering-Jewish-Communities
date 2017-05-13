

from lxml import etree
import seaborn as sns

IMG_DIR = '../Images/'  # empty string if in same directory
MAP_TEMPLATE_FP = ''.join([IMG_DIR, 'counties_map_template.svg'])

# TODO: make class, add functionality for FIPS highlight without any other data
def draw_county_data_svg(data, fp, colors=6, bins=None,
                         template=MAP_TEMPLATE_FP,
                         zero_color=None, no_data_color='#eeeeee',
                         make_key=True):

    # Path styles
    county_style = (
        'fill-rule:nonzero;'
        'stroke:#6e6e6e;'
        'stroke-opacity:1;'
        'stroke-width:0.1;'
        'stroke-miterlimit:4;'
        'stroke-dasharray:none;'
        'stroke-linecap:butt;'
        'marker-start:none;'
        'stroke-linejoin:bevel;'
        'fill:'  # to be determined
        )
    state_style = (
        'fill:none;'
        'stroke:#221e1f;'
        'stroke-width:0.3;'
        'stroke-linecap:butt;'
        'stroke-linejoin:round;'
        'stroke-miterlimit:4'
        )
    sep_style = (
        'fill:none;'
        'stroke:#a9a9a9;'
        'stroke-width:1.29999995'
        )

    assert type(bins) is list or type(None)
    assert type(colors) is int or list

    if bins is None:
        if type(colors) is int:
            n_colors = colors
            color_pal = sns.color_palette('Reds', n_colors)
            colors = color_pal.as_hex()
        else:
            n_colors = len(colors)

        # quantile values start first quantile and end with max
        bins = [data.quantile((x+1) / n_colors) for x in range(n_colors)]

    else:
        if type(colors) is int:
            n_colors = len(bins) if len(bins) <= colors else colors
            color_pal = sns.color_palette('Reds', n_colors)
            colors = color_pal.as_hex()

    if zero_color is not None:
        zero_color = no_data_color if zero_color is True else zero_color
        bins.insert(0, data.min())
        colors.insert(0, zero_color)

    n_bins = len(bins)
    n_colors = len(colors)
    # TODO: bug for draw_county_data_svg(data, DIR,
    # bins=list(range(1, 9))+[10], colors=10, zero_color=True)
    # when colors is left as default (6), raises Assertion error
    assert n_bins == n_colors, (
        '{} colors passed for {} bins, must be equal.'.format(n_colors, n_bins)
        )

    svg = etree.ElementTree(file=template)
    for p in svg.iterfind('.//{http://www.w3.org/2000/svg}path'):
        id_ = p.attrib['id']
        try:
            val = data.at[id_]
        except KeyError:
            if id_=='state_lines':
                style = state_style
            elif id_=='separator':
                style = sep_style
            else:  # if path is for a county not present in dataset
                style = ''.join([county_style, no_data_color])
        else:
            # print(id_, val)
            for bin_, color in zip(bins, colors):
                if val <= bin_:
                    style = ''.join([county_style, color])
                    break
        p.attrib['style'] = style

    if make_key:
        pass

    svg.write(fp, pretty_print=True)

    try:
        return sns.palplot(color_pal)
    except UnboundLocalError:
        pass
